"""Windowed count aggregator.

One use case is defined by:

  * aggregation_level (cfg.aggregation_level) - the field to GROUP by,
    e.g. "facility". Its value in an event -> output "entity".
  * metric_names      (cfg.metric_names)      - one or more fields to BREAK
    DOWN the group by, e.g. ("severity", "tag"). Each is
    aggregated independently; a value in an event -> output "metric_value".

Events are bucketed by (window, entity, metric_name, metric_value) and
counted. For each metric the share denominator is ALL values of THAT metric
for the entity (every event has exactly one value per metric field, so the
sum of a metric's buckets == the entity's event count).

Example: grouping by facility, breaking down by severity and tag,
yields per window one doc per (facility value, metric, metric value):
facility=daemon & severity=warning, facility=daemon & tag=sshd, ...

When a window is in the past, one output document is emitted per bucket:

    timestamp            end of window (ISO 8601, UTC)
    frequency            configured window, e.g. "1m"
    aggregation_level    field grouped on, e.g. "facility"
    entity               value of the aggregation field, e.g. "daemon"
    metric_name          the metric field, e.g. "severity"
    metric_value         value of that field, e.g. "warning"
    n                    events with this (entity, metric, metric_value)
    value_type           "count" | "share"
    value                count -> n
                         share -> n / all events for this entity and metric_name
                                in the window
    source               source data set, e.g. "syslog"
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from itertools import groupby

from .config import UsecaseConfig
from .fields import extract

log = logging.getLogger(__name__)


def parse_iso(ts: str) -> float:
    """Parse an ISO 8601 timestamp (with 'Z' supported) to epoch seconds."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


class WindowAggregator:
    def __init__(self, cfg: UsecaseConfig, now_fn=time.time):
        self.cfg = cfg
        self.now_fn = now_fn
        # (window, entity, metric_name, metric_value) -> event count
        self.state: dict[tuple[int, str, str, str], int] = defaultdict(int)

    # -- windowing -------------------------------------------------------
    def _window_start(self, epoch: float) -> int:
        w = self.cfg.freq_seconds
        return int(epoch) - (int(epoch) % w)

    # -- processing ------------------------------------------------------
    def process(self, event: dict, event_time: float | None = None) -> None:
        if event_time is None:
            ts = event.get(self.cfg.timestamp_field)
            if isinstance(ts, str):
                try:
                    event_time = parse_iso(ts)
                except ValueError:
                    log.warning("bad %s %r, using now", self.cfg.timestamp_field, ts)
                    event_time = self.now_fn()
            else:
                event_time = self.now_fn()
        window = self._window_start(event_time)

        entity = extract(event, self.cfg.aggregation_level)
        for metric_name in self.cfg.metric_names:
            metric_value = extract(event, metric_name)
            self.state[(window, entity, metric_name, metric_value)] += 1

    # -- flushing --------------------------------------------------------
    def flush_expired(self, now: float | None = None) -> list[tuple[str, dict]]:
        """Return (doc_id, doc) for all windows that have ended, forget them."""
        now = self.now_fn() if now is None else now
        w = self.cfg.freq_seconds
        expired = [k for k, cnt in self.state.items() if cnt > 0 and now >= k[0] + w]
        return self._emit(expired)

    def flush_all(self) -> list[tuple[str, dict]]:
        """Flush every window, even unfinished ones (used on shutdown)."""
        return self._emit([k for k, cnt in self.state.items() if cnt > 0])

    def _emit(self, keys: list[tuple[int, str, str, str]]) -> list[tuple[str, dict]]:
        # Group buckets by (window, entity, metric_name): that group sum across
        # metric_value is the share denominator (all events for this entity and
        # metric_name in the window). sorted() is required for groupby to work.
        out: list[tuple[str, dict]] = []
        for _group_key, group_iter in groupby(sorted(keys), key=lambda k: k[:3]):
            group = list(group_iter)
            total = sum(self.state[k] for k in group)
            for key in group:
                out.append((self._doc_id(*key), self._doc(*key, self.state[key], total)))
                del self.state[key]
        return out

    # -- output ----------------------------------------------------------
    def _doc(
        self,
        window: int,
        entity: str,
        metric_name: str,
        metric_value: str,
        n: int,
        metric_total: int,
    ) -> dict:
        w = self.cfg.freq_seconds
        result = (n / metric_total) if self.cfg.value_type == "share" else n
        end = datetime.fromtimestamp(window + w, tz=timezone.utc)
        return {
            "timestamp": end.isoformat().replace("+00:00", "Z"),
            "frequency": self.cfg.freq,
            "aggregation_level": self.cfg.aggregation_level,
            "entity": entity,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "n": n,
            "value_type": self.cfg.value_type,
            "value": result,
            "source": self.cfg.source,
        }

    def _doc_id(self, window: int, entity: str, metric_name: str, metric_value: str) -> str:
        # idempotent: same window+entity+metric+metric_value rewrites the doc
        return f"{window}|{entity}|{metric_name}|{metric_value}"
