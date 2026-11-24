from datetime import datetime, timezone

import pytest

from agg_pipeline.aggregator import WindowAggregator
from agg_pipeline.config import UsecaseConfig

# Default: group by "facility", break down by the given metric field(s)


def make_cfg(
    freq="1m", agg_field="facility", metric_fields=("severity",), value_type="share", **kw
) -> UsecaseConfig:
    base = dict(
        name="t",
        topic="t",
        consumer_group="g",
        source="test_src",
        freq=freq,
        freq_seconds=int(freq[:-1]) * {"s": 1, "m": 60, "h": 3600}[freq[-1]],
        aggregation_level=agg_field,
        metric_names=tuple(metric_fields),
        value_type=value_type,
        index_name="metrics",
        kafka_bootstrap_servers="b",
        es_hosts=["u"],
    )
    base.update(kw)
    return UsecaseConfig(**base)


def ev(ts: str, **fields) -> dict:
    return {"timestamp": ts} | fields


def t0() -> int:
    return int(datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())


def _flush(agg, base, offset):
    return {
        (d["entity"], d["metric_name"], d["metric_value"]): d
        for _, d in agg.flush_expired(now=base + offset)
    }


def test_count_and_share_single_metric():
    cfg = make_cfg(freq="1m")
    agg = WindowAggregator(cfg)
    base = t0()
    for s, fac, sev in (
        (10, "daemon", "err"),
        (20, "daemon", "err"),
        (30, "daemon", "info"),
        (40, "kern", "err"),
    ):
        agg.process(ev(f"2026-01-01T00:00:{s:02d}Z", facility=fac, severity=sev))

    docs = _flush(agg, base, 61)
    assert set(docs) == {
        ("daemon", "severity", "err"),
        ("daemon", "severity", "info"),
        ("kern", "severity", "err"),
    }

    d = docs[("daemon", "severity", "err")]
    assert d["n"] == 2
    assert d["value"] == pytest.approx(2 / 3)  # of the 3 daemon events
    # document schema
    assert d["timestamp"] == "2026-01-01T00:01:00Z"
    assert d["frequency"] == "1m"
    assert d["aggregation_level"] == "facility"
    assert d["entity"] == "daemon"
    assert d["metric_name"] == "severity"
    assert d["metric_value"] == "err"
    assert d["value_type"] == "share"
    assert d["source"] == "test_src"


def test_multiple_metrics_independent():
    """Each metric is broken down independently within its own values."""
    cfg = make_cfg(freq="1m", metric_fields=("severity", "tag"))
    agg = WindowAggregator(cfg)
    base = t0()
    # 3 events, all facility=daemon
    agg.process(ev("2026-01-01T00:00:10Z", facility="daemon", severity="err", tag="sshd"))
    agg.process(ev("2026-01-01T00:00:20Z", facility="daemon", severity="err"))  # tag missing
    agg.process(ev("2026-01-01T00:00:30Z", facility="daemon", severity="info"))  # tag missing

    docs = _flush(agg, base, 61)

    # severity metric: denominator = 3 (all daemon events)
    assert docs[("daemon", "severity", "err")]["n"] == 2
    assert docs[("daemon", "severity", "err")]["value"] == pytest.approx(2 / 3)
    assert docs[("daemon", "severity", "info")]["n"] == 1
    assert docs[("daemon", "severity", "info")]["value"] == pytest.approx(1 / 3)

    # tag metric: denominator = 3 (all daemon events), "unknown" for the 2 missing
    assert docs[("daemon", "tag", "sshd")]["n"] == 1
    assert docs[("daemon", "tag", "sshd")]["value"] == pytest.approx(1 / 3)
    assert docs[("daemon", "tag", "unknown")]["n"] == 2
    assert docs[("daemon", "tag", "unknown")]["value"] == pytest.approx(2 / 3)

    assert len(docs) == 4


def test_count_value_type():
    cfg = make_cfg(freq="1m", value_type="count")
    agg = WindowAggregator(cfg)
    base = t0()
    for s, fac, sev in ((10, "daemon", "err"), (20, "daemon", "err"), (30, "kern", "err")):
        agg.process(ev(f"2026-01-01T00:00:{s:02d}Z", facility=fac, severity=sev))
    docs = _flush(agg, base, 61)
    assert docs[("daemon", "severity", "err")]["value"] == 2
    assert docs[("kern", "severity", "err")]["value"] == 1
    assert docs[("daemon", "severity", "err")]["value_type"] == "count"


def test_missing_fields_are_unknown():
    cfg = make_cfg(freq="1m")
    agg = WindowAggregator(cfg)
    base = t0()
    agg.process(ev("2026-01-01T00:00:10Z"))  # no facility, no severity
    ((_id, d),) = agg.flush_expired(now=base + 61)
    assert d["entity"] == "unknown"
    assert d["metric_value"] == "unknown"


def test_no_flush_before_window_end():
    cfg = make_cfg(freq="1m")
    agg = WindowAggregator(cfg)
    base = t0()
    agg.process(ev("2026-01-01T00:00:10Z", facility="daemon", severity="err"))
    assert agg.flush_expired(now=base + 30) == []
    assert len(agg.state) == 1


def test_windows_are_separate():
    cfg = make_cfg(freq="1m")
    agg = WindowAggregator(cfg)
    base = t0()
    agg.process(ev("2026-01-01T00:00:59Z", facility="daemon", severity="err"))
    agg.process(ev("2026-01-01T00:01:00Z", facility="daemon", severity="err"))
    docs = agg.flush_expired(now=base + 120)
    ts = sorted(d["timestamp"] for _, d in docs)
    assert ts == ["2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"]
    assert len(agg.state) == 0


def test_5m_window():
    cfg = make_cfg(freq="5m")
    agg = WindowAggregator(cfg)
    base = t0()
    agg.process(ev("2026-01-01T00:03:00Z", facility="daemon", severity="err"))
    docs = agg.flush_expired(now=base + 300)  # window [00:00, 00:05) ends
    assert len(docs) == 1
    _, d = docs[0]
    assert d["timestamp"] == "2026-01-01T00:05:00Z"
    assert d["frequency"] == "5m"
    assert d["value"] == pytest.approx(1.0)


def test_dotted_metric_field():
    cfg = make_cfg(freq="1m", metric_fields=("process.program",))
    agg = WindowAggregator(cfg)
    base = t0()
    for s, program in ((10, "ls"), (20, "cp"), (30, "ls")):
        e = ev(f"2026-01-01T00:00:{s:02d}Z", facility="daemon")
        e["process.program"] = program  # flat dotted field, not a nested dict
        agg.process(e)
    docs = _flush(agg, base, 61)
    assert docs[("daemon", "process.program", "ls")]["n"] == 2
    assert docs[("daemon", "process.program", "cp")]["n"] == 1


def test_doc_id_is_stable():
    cfg = make_cfg(freq="1m")
    agg = WindowAggregator(cfg)
    base = t0()
    agg.process(ev("2026-01-01T00:00:10Z", facility="daemon", severity="err"))
    ((id1, _),) = agg.flush_expired(now=base + 61)
    agg.process(ev("2026-01-01T00:01:20Z", facility="daemon", severity="err"))
    ((id2, _),) = agg.flush_expired(now=base + 120)  # different window -> different id
    agg.process(ev("2026-01-01T00:00:30Z", facility="daemon", severity="err"))
    ((id3, _),) = agg.flush_expired(now=base + 3610)
    assert id1 == f"{base}|daemon|severity|err"
    assert id1 != id2
    assert id1 == id3  # same window+entity+metric+metric_value -> same id


def test_flush_all_unfinished_windows():
    cfg = make_cfg(freq="1m")
    agg = WindowAggregator(cfg)
    base = t0()
    agg.process(ev("2026-01-01T00:00:10Z", facility="daemon", severity="err"))
    assert agg.flush_expired(now=base + 30) == []
    docs = agg.flush_all()
    assert len(docs) == 1
    assert docs[0][1]["timestamp"] == "2026-01-01T00:01:00Z"


def test_missing_timestamp_falls_back_to_now():
    cfg = make_cfg(freq="1m")
    fixed = float(t0() + 20)
    agg = WindowAggregator(cfg, now_fn=lambda: fixed)
    agg.process({"facility": "daemon", "severity": "err"})
    docs = agg.flush_expired(now=fixed + 40)
    assert len(docs) == 1
    _, d = docs[0]
    assert d["timestamp"] == "2026-01-01T00:01:00Z"
