"""End-to-end pipeline test with in-memory consumer and ES writer fakes."""

import threading
from typing import Any

import pytest

from agg_pipeline.config import UsecaseConfig
from agg_pipeline.pipeline import Pipeline

BASE: dict[str, Any] = {
    "name": "t",
    "topic": "t",
    "consumer_group": "g",
    "source": "test_src",
    "freq": "1m",
    "freq_seconds": 60,
    "index_name": "metrics",
    "kafka_bootstrap_servers": "b",
    "es_hosts": ["u"],
}


def make_cfg(**kw: Any) -> UsecaseConfig:
    d: dict[str, Any] = {
        "timestamp_field": "timestamp",
        "value_type": "share",
        "aggregation_level": "facility",
        "metric_names": ("severity",),
    }
    d.update(BASE)
    d.update(kw)
    return UsecaseConfig(**d)


class FakeConsumer:
    def __init__(self, events: list[dict]):
        self.events = list(events)
        self.closed = False
        self.stop_event: threading.Event | None = None  # set to pipeline._stop

    def poll(self, timeout: float = 1.0):
        if self.events:
            return self.events.pop(0)
        if self.stop_event is not None:
            self.stop_event.set()  # nothing left -> ask pipeline to stop
        return None

    def close(self):
        self.closed = True


class FakeWriter:
    def __init__(self):
        self.calls: list[list[tuple[str, dict]]] = []

    def write(self, docs):
        if docs:
            self.calls.append(docs)


def _all_docs(writer):
    return [d for call in writer.calls for _, d in call]


def _key(d):
    return (d["entity"], d["metric_name"], d["metric_value"])


def test_pipeline_end_to_end_expired_windows():
    """Backfilled (past-window) events are flushed as they arrive.

    Each flush batch is its own closed batch, so for backfilled data every
    doc's share denominator is the events seen in that batch (1 -> 1.0).
    """
    cfg = make_cfg()
    events = [
        {"timestamp": "2026-01-01T00:00:10Z", "facility": "daemon", "severity": "err"},
        {"timestamp": "2026-01-01T00:00:20Z", "facility": "daemon", "severity": "info"},
        {"timestamp": "2026-01-01T00:00:30Z", "facility": "kern", "severity": "err"},
    ]
    cons, writer = FakeConsumer(events), FakeWriter()
    pl = Pipeline(cfg, consumer=cons, writer=writer)
    cons.stop_event = pl._stop
    pl.run(poll_timeout=0)

    assert cons.closed
    all_docs = _all_docs(writer)
    assert len(all_docs) == 3
    for d in all_docs:
        assert d["timestamp"] == "2026-01-01T00:01:00Z"
        assert d["n"] == 1
        assert d["value"] == 1.0
        assert d["aggregation_level"] == "facility"
        assert d["metric_name"] == "severity"
        assert d["frequency"] == "1m"
        assert d["value_type"] == "share"
    assert {_key(d) for d in all_docs} == {
        ("daemon", "severity", "err"),
        ("daemon", "severity", "info"),
        ("kern", "severity", "err"),
    }


def test_pipeline_accumulates_and_flushes_on_shutdown():
    """Events inside the current window accumulate and are flushed at shutdown."""
    cfg = make_cfg()
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    events = [
        {"timestamp": ts, "facility": "daemon", "severity": "err"},
        {"timestamp": ts, "facility": "daemon", "severity": "err"},
        {"timestamp": ts, "facility": "daemon", "severity": "info"},
    ]
    cons, writer = FakeConsumer(events), FakeWriter()
    pl = Pipeline(cfg, consumer=cons, writer=writer)
    cons.stop_event = pl._stop
    pl.run(poll_timeout=0)

    docs = _all_docs(writer)
    assert len(docs) == 2  # one doc per (entity, metric_value)
    keys = {_key(d): d for d in docs}
    assert keys[("daemon", "severity", "err")]["n"] == 2
    assert keys[("daemon", "severity", "err")]["value"] == pytest.approx(2 / 3)
    assert keys[("daemon", "severity", "info")]["value"] == pytest.approx(1 / 3)


def test_pipeline_multiple_metrics():
    """Each configured metric yields its own set of docs, same denominator."""
    cfg = make_cfg(metric_names=("severity", "tag"))
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    events = [
        {"timestamp": ts, "facility": "daemon", "severity": "err", "tag": "sshd"},
        {"timestamp": ts, "facility": "daemon", "severity": "err"},  # tag missing
        {"timestamp": ts, "facility": "daemon", "severity": "info"},  # tag missing
    ]
    cons, writer = FakeConsumer(events), FakeWriter()
    pl = Pipeline(cfg, consumer=cons, writer=writer)
    cons.stop_event = pl._stop
    pl.run(poll_timeout=0)

    docs = _all_docs(writer)
    keys = {_key(d): d for d in docs}
    # 2 severity values + 2 tag values (sshd, unknown)
    assert set(keys) == {
        ("daemon", "severity", "err"),
        ("daemon", "severity", "info"),
        ("daemon", "tag", "sshd"),
        ("daemon", "tag", "unknown"),
    }
    # both metrics share the denominator of 3 daemon events
    assert keys[("daemon", "severity", "err")]["value"] == pytest.approx(2 / 3)
    assert keys[("daemon", "tag", "sshd")]["value"] == pytest.approx(1 / 3)
    assert keys[("daemon", "tag", "unknown")]["n"] == 2


def test_pipeline_two_dimensions_count():
    """Multiple entities x metric values, count value_type."""
    cfg = make_cfg(value_type="count")
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    events = [
        {"timestamp": ts, "facility": "daemon", "severity": "err"},
        {"timestamp": ts, "facility": "daemon", "severity": "info"},
        {"timestamp": ts, "facility": "kern", "severity": "err"},
        {"timestamp": ts, "facility": "kern", "severity": "err"},
    ]
    cons, writer = FakeConsumer(events), FakeWriter()
    pl = Pipeline(cfg, consumer=cons, writer=writer)
    cons.stop_event = pl._stop
    pl.run(poll_timeout=0)

    docs = _all_docs(writer)
    keys = {_key(d): d for d in docs}
    assert set(keys) == {
        ("daemon", "severity", "err"),
        ("daemon", "severity", "info"),
        ("kern", "severity", "err"),
    }
    assert keys[("daemon", "severity", "err")]["value"] == 1
    assert keys[("kern", "severity", "err")]["n"] == 2
    assert keys[("kern", "severity", "err")]["value"] == 2
    for d in docs:
        assert d["value_type"] == "count"
