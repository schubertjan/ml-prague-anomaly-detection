"""Config loading for aggregation use cases.

Each use case is described by a YAML file. One config -> one consumer,
one consumer group, one aggregation pipeline, one ES index.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, replace

import yaml

_FREQ_RE = re.compile(r"^(\d+)([smh])$")
_FREQ_UNITS = {"s": 1, "m": 60, "h": 3600}


def parse_freq(spec: str) -> int:
    """Parse a window spec like '30s', '5m', '2h' into seconds."""
    m = _FREQ_RE.match(spec.strip())
    if not m:
        raise ValueError(f"invalid freq {spec!r}, expected <int><s|m|h> e.g. 1m, 5m")
    return int(m.group(1)) * _FREQ_UNITS[m.group(2)]


@dataclass(frozen=True)
class UsecaseConfig:
    name: str
    topic: str
    consumer_group: str
    source: str
    freq: str  # original spec, e.g. "1m" (-> output "frequency")
    freq_seconds: int
    # The field(s) of a use case:
    #   aggregation_level - the field to GROUP by, e.g. "location".
    #                       -> output "aggregation_level"; each value -> "entity".
    #   metric_names      - one or more fields to BREAK DOWN the group by,
    #                       e.g. ["severity", "tag"]. Each is
    #                       aggregated independently: -> output "metric_name"
    #                       (that field), each value -> "metric_value". One
    #                       doc per (entity, metric, metric_value) per window.
    aggregation_level: str
    metric_names: tuple[str, ...]
    value_type: str  # "share" | "count"
    index_name: str  # the exact ES index documents are written into
    kafka_bootstrap_servers: str  # from kafka.bootstrap_servers
    es_hosts: list[str]  # elasticsearch.hosts
    kafka_consumer: dict = field(
        default_factory=dict
    )  # raw kafka.consumer sub-block (group_id, SSL, ...)
    timestamp_field: str = "timestamp"

    def es_settings(self) -> dict:
        return {
            "index.mapping.total_fields.limit": "100",
            "index.number_of_shards": "1",
            "index.number_of_replicas": "0",
        }


def _require(d: dict, key: str, ctx: str):
    if key not in d or d[key] in (None, ""):
        raise ValueError(f"missing required config key {key!r} in {ctx}")
    return d[key]


def _metric_names(raw: dict, ctx: str) -> tuple[str, ...]:
    """metric_names: a non-empty list of field names to break down the group by."""
    m = _require(raw, "metric_names", ctx)
    if not isinstance(m, list) or not m or any(not isinstance(v, (str, int, float)) for v in m):
        raise ValueError(f"invalid metric_names in {ctx}, expected a non-empty list of field names")
    return tuple(str(v) for v in m)


def _hosts(es: dict, ctx: str) -> list[str]:
    """elasticsearch.hosts: a non-empty list of host URLs (a single string is accepted)."""
    h = _require(es, "hosts", f"{ctx} (elasticsearch)")
    if isinstance(h, str):
        h = [h]
    if not isinstance(h, list) or not h or any(not str(v).strip() for v in h):
        raise ValueError(
            f"invalid elasticsearch.hosts in {ctx}, expected a non-empty list of host URLs"
        )
    return [str(v) for v in h]


def load_config(path: str) -> UsecaseConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"config {path} must be a mapping")

    uc = _require(raw, "use_case", path)
    kafka = _require(raw, "kafka", path)
    es = _require(raw, "elasticsearch", path)
    uc_ctx = f"{path} (use_case)"

    index_name = str(_require(uc, "index_name", uc_ctx))
    freq = _require(uc, "freq", uc_ctx)
    freq_seconds = parse_freq(freq)
    es_hosts = _hosts(es, path)

    value_type = str(uc.get("value_type", "share")).lower()
    if value_type not in ("share", "count"):
        raise ValueError(f"invalid value_type {value_type!r}, expected 'share' or 'count'")

    metric_names = _metric_names(uc, uc_ctx)

    topic = str(_require(kafka, "topic", f"{path} (kafka)"))
    kafka_bootstrap_servers = str(_require(kafka, "bootstrap_servers", f"{path} (kafka)"))
    kafka_consumer = dict(kafka.get("consumer") or {})
    consumer_group = str(_require(kafka_consumer, "group_id", f"{path} (kafka.consumer)"))

    cfg = UsecaseConfig(
        name=str(_require(uc, "name", uc_ctx)),
        topic=topic,
        consumer_group=consumer_group,
        source=str(_require(uc, "source", uc_ctx)),
        freq=str(freq),
        freq_seconds=freq_seconds,
        aggregation_level=str(_require(uc, "aggregation_level", uc_ctx)),
        metric_names=metric_names,
        value_type=value_type,
        index_name=index_name,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        es_hosts=es_hosts,
        kafka_consumer=kafka_consumer,
        timestamp_field=str(uc.get("timestamp_field", "timestamp")),
    )

    # Optional runtime overrides (used by docker compose to reach the
    # containers at kafka:9092 / elasticsearch:9200 instead of localhost).
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    hosts = os.environ.get("ES_HOSTS")
    if brokers:
        cfg = replace(cfg, kafka_bootstrap_servers=brokers)
    if hosts:
        cfg = replace(cfg, es_hosts=[h.strip() for h in hosts.split(",") if h.strip()])
    return cfg
