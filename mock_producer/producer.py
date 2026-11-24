"""Mock kafka producer: pushes dummy documents for a topic.

python -m mock_producer --config configs/producer_syslog.yaml
python -m mock_producer --config configs/producer_syslog.yaml --count 20
python -m mock_producer --config configs/producer_syslog.yaml --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from .generator import generate

log = logging.getLogger(__name__)


def load_producer_config(path: str) -> dict:
    import yaml

    with open(path) as f:
        return yaml.safe_load(f)


# Friendly YAML keys (reference kafka config) -> librdkafka config keys.
_KAFKA_KEY_MAP = {
    "enable_idempotence": "enable.idempotence",
    "acks": "acks",
    "security_protocol": "security.protocol",
    "ssl_certfile": "ssl.certificate.location",
    "ssl_keyfile": "ssl.key.location",
    "ssl_cafile": "ssl.ca.location",
}


def _make_producer(bootstrap_servers: str, producer_cfg: dict | None = None):
    from confluent_kafka import Producer

    delivered = {"n": 0}

    def on_delivery(err, msg):
        if err:
            log.warning("delivery failed: %s", err)
        else:
            delivered["n"] += 1

    conf = {"bootstrap.servers": bootstrap_servers, "socket.timeout.ms": 10_000}
    for k, v in (producer_cfg or {}).items():
        if v is not None:
            conf[_KAFKA_KEY_MAP.get(k, k)] = v
    p = Producer(conf)
    return p, delivered, on_delivery


def run(
    path: str,
    count: int | None = None,
    dry_run: bool = False,
    rate_override: float | None = None,
    limit_log: int = 5,
) -> int:
    cfg = load_producer_config(path)
    kafka = cfg.get("kafka") or {}
    topic = kafka["topic"]
    rate = float(rate_override if rate_override is not None else cfg.get("rate", 0))
    total = count or int(cfg.get("count", 0))  # 0 = unlimited
    duration = float(cfg.get("duration_seconds", 0))  # 0 = unlimited
    spec = cfg.get("fields") or {}

    produced = 0
    logged = 0
    t0 = time.time()
    p, delivered, on_delivery = (
        (None, {"n": 0}, None)
        if dry_run
        else _make_producer(kafka["bootstrap_servers"], kafka.get("producer") or {})
    )

    log.info(
        "topic=%s dry_run=%s rate=%s/s count=%s duration=%ss",
        topic,
        dry_run,
        rate,
        total or "inf",
        duration or "inf",
    )
    try:
        while True:
            doc = generate(spec)

            if dry_run:
                if logged < limit_log:
                    log.info("-> %s", json.dumps(doc))
                    logged += 1
            else:
                assert p is not None
                p.produce(topic, value=json.dumps(doc).encode(), on_delivery=on_delivery)
                p.poll(0)

            produced += 1
            if total and produced >= total:
                break
            if duration and time.time() - t0 >= duration:
                break
            if rate > 0:
                time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        log.info("interrupted after %d docs", produced)
    finally:
        if p is not None:
            p.flush(30)
            log.info("delivered %d docs to %s", delivered["n"], topic)

    return produced


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Push dummy documents to a kafka topic")
    ap.add_argument("--config", required=True, help="producer YAML config")
    ap.add_argument(
        "--count", type=int, default=0, help="produce exactly N docs (overrides config)"
    )
    ap.add_argument("--rate", type=float, default=None, help="docs per second (overrides config)")
    ap.add_argument(
        "--dry-run", action="store_true", help="do not connect to kafka, just print sample docs"
    )
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(args.config, count=args.count or None, dry_run=args.dry_run, rate_override=args.rate)


if __name__ == "__main__":
    main()
