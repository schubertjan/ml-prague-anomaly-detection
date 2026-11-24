from pathlib import Path

import pytest

from agg_pipeline.config import load_config, parse_freq

REPO = Path(__file__).resolve().parent.parent

# Shared infra blocks (kafka + elasticsearch) used by the inline fixtures.
INFRA = (
    "kafka:\n  bootstrap_servers: b\n  topic: t\n  consumer:\n    group_id: g\n"
    "elasticsearch:\n  hosts:\n    - u\n"
)


def _uc_yaml(body: str) -> str:
    """Wrap use-case fields (already 2-space indented) in a use_case: block."""
    return "use_case:\n" + body + INFRA


@pytest.mark.parametrize("spec,seconds", [("30s", 30), ("1m", 60), ("5m", 300), ("1h", 3600)])
def test_parse_freq(spec, seconds):
    assert parse_freq(spec) == seconds


@pytest.mark.parametrize("spec", ["1x", "m", "1", "", "m1"])
def test_parse_freq_invalid(spec):
    with pytest.raises(ValueError):
        parse_freq(spec)


def test_load_config_syslog():
    cfg = load_config(str(REPO / "configs" / "syslog.yaml"))
    assert cfg.name == "syslog"
    assert cfg.topic == "syslog-events"
    assert cfg.consumer_group == "agg-syslog"
    assert cfg.kafka_bootstrap_servers == "localhost:9092"
    assert cfg.kafka_consumer["group_id"] == "agg-syslog"
    assert cfg.source == "syslog"
    assert cfg.freq == "1m"
    assert cfg.freq_seconds == 60
    assert cfg.aggregation_level == "facility"
    assert cfg.metric_names == ("severity", "tag")
    assert cfg.value_type == "share"
    assert cfg.index_name == "syslog-1m"
    assert cfg.es_hosts == ["http://localhost:9200"]


def test_load_config_access_log():
    cfg = load_config(str(REPO / "configs" / "access_log.yaml"))
    assert cfg.freq_seconds == 300
    assert cfg.aggregation_level == "path"
    assert cfg.metric_names == ("status_code",)
    assert cfg.value_type == "count"
    assert cfg.index_name == "access-5m"
    assert cfg.es_hosts == ["http://localhost:9200"]


def _write(tmp_path, text: str) -> str:
    p = tmp_path / "cfg.yaml"
    p.write_text(text)
    return str(p)


def test_missing_required_key(tmp_path):
    # kafka.topic is missing
    with pytest.raises(ValueError, match="topic"):
        load_config(
            _write(
                tmp_path,
                "use_case:\n  name: x\n  source: s\n  freq: 1m\n  aggregation_level: a\n"
                "  metric_names: [m]\n  index_name: i\n"
                "kafka:\n  bootstrap_servers: b\n  consumer:\n    group_id: g\n"
                "elasticsearch:\n  hosts:\n    - u\n",
            )
        )


def test_missing_aggregation_level(tmp_path):
    with pytest.raises(ValueError, match="aggregation_level"):
        load_config(
            _write(
                tmp_path,
                _uc_yaml(
                    "  name: x\n  source: s\n  freq: 1m\n  metric_names: [m]\n  index_name: i\n"
                ),
            )
        )


def test_missing_metric_names(tmp_path):
    with pytest.raises(ValueError, match="metric_names"):
        load_config(
            _write(
                tmp_path,
                _uc_yaml(
                    "  name: x\n  source: s\n  freq: 1m\n  aggregation_level: a\n  index_name: i\n"
                ),
            )
        )


def test_empty_metric_names_rejected(tmp_path):
    with pytest.raises(ValueError, match="metric_names"):
        load_config(
            _write(
                tmp_path,
                _uc_yaml(
                    "  name: x\n  source: s\n  freq: 1m\n  aggregation_level: a\n"
                    "  metric_names: []\n  index_name: i\n"
                ),
            )
        )


def test_bad_value_type(tmp_path):
    with pytest.raises(ValueError, match="value_type"):
        load_config(
            _write(
                tmp_path,
                _uc_yaml(
                    "  name: x\n  source: s\n  freq: 1m\n  aggregation_level: a\n"
                    "  metric_names: [m]\n  value_type: bogus\n  index_name: i\n"
                ),
            )
        )


def test_missing_elasticsearch_hosts(tmp_path):
    with pytest.raises(ValueError, match="hosts"):
        load_config(
            _write(
                tmp_path,
                "use_case:\n  name: x\n  source: s\n  freq: 1m\n  aggregation_level: a\n"
                "  metric_names: [m]\n  index_name: i\n"
                "kafka:\n  bootstrap_servers: b\n  topic: t\n  consumer:\n    group_id: g\n"
                "elasticsearch: {}\n",
            )
        )


def test_index_name_used_verbatim(tmp_path):
    cfg = load_config(
        _write(
            tmp_path,
            _uc_yaml(
                "  name: x\n  source: s\n  freq: 5m\n  aggregation_level: a\n"
                "  metric_names: [m]\n  index_name: custom-index\n"
            ),
        )
    )
    assert cfg.index_name == "custom-index"


def test_env_overrides(tmp_path, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("ES_HOSTS", " http://es1:9200,http://es2:9200 ")
    cfg = load_config(
        _write(
            tmp_path,
            _uc_yaml(
                "  name: x\n  source: s\n  freq: 1m\n  aggregation_level: a\n"
                "  metric_names: [m]\n  index_name: i\n"
            ),
        )
    )
    assert cfg.kafka_bootstrap_servers == "kafka:9092"
    assert cfg.es_hosts == ["http://es1:9200", "http://es2:9200"]


def test_env_overrides_absent(tmp_path, monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("ES_HOSTS", raising=False)
    cfg = load_config(
        _write(
            tmp_path,
            _uc_yaml(
                "  name: x\n  source: s\n  freq: 1m\n  aggregation_level: a\n"
                "  metric_names: [m]\n  index_name: i\n"
            ),
        )
    )
    assert cfg.kafka_bootstrap_servers == "b"
    assert cfg.es_hosts == ["u"]
