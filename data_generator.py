"""Standalone mock streaming data generator: pushes dummy syslog events to a Kafka topic.

Single file, no config file -- everything is a CLI flag.

Examples:
    python data_generator.py --dry-run                       # just print sample docs
    python data_generator.py --topic syslog-events           # 100 docs/s until Ctrl-C
    python data_generator.py --topic syslog-events --rate 500 --count 100000
    python data_generator.py --topic syslog-events --rate 0  # as fast as possible
    python data_generator.py --topic syslog-events --duration 60
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

log = logging.getLogger("data_generator")

# Mock event schema: a syslog-style event, same shape the aggregation
# pipeline expects on topic syslog-events (see configs/syslog.yaml).
FACILITIES = ["kern", "auth", "daemon", "cron", "local0", "user"]
# duplicates act as weights: mostly info/notice, rarely crit/alert/emerg
SEVERITIES = [
    "info",
    "info",
    "info",
    "info",
    "notice",
    "notice",
    "warning",
    "warning",
    "err",
    "debug",
    "crit",
    "alert",
    "emerg",
]
TAGS = ["cron", "sshd", "systemd", "nginx", "kernel", "su", "named", "dbus"]
MESSAGES = [
    "session opened for user deploy",
    "Failed password for root from 203.0.113.7 port 44122 ssh2",
    "CRON (root) CMD (/usr/local/lib/cleanup.sh)",
    "Started Daily apt download activities.",
    "eth0: link up (full-duplex, 1000Mbps)",
    "Accepted publickey for deploy from 198.51.100.9 port 51234 ssh2",
    "worker process exited on signal",
    "New session 42 of user deploy",
]


def random_doc() -> dict:
    return {
        "timestamp": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "facility": random.choice(FACILITIES),
        "severity": random.choice(SEVERITIES),
        "tag": random.choice(TAGS),
        "host_name": f"svr-{random.randint(0, 19)}",
        "message": random.choice(MESSAGES),
    }


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Generate mock events and stream them to a Kafka topic."
    )
    ap.add_argument("--topic", help="Kafka topic to produce to")
    ap.add_argument(
        "--brokers", default="localhost:9092", help="bootstrap servers (default: localhost:9092)"
    )
    ap.add_argument(
        "--rate",
        type=float,
        default=100,
        help="docs per second, 0 = as fast as possible (default: 100)",
    )
    ap.add_argument(
        "--count", type=int, default=0, help="stop after N docs, 0 = unlimited (default: 0)"
    )
    ap.add_argument(
        "--duration", type=float, default=0, help="stop after N seconds, 0 = unlimited (default: 0)"
    )
    ap.add_argument("--seed", type=int, default=None, help="random seed for reproducible output")
    ap.add_argument(
        "--dry-run", action="store_true", help="print sample docs, do not connect to Kafka"
    )
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.seed is not None:
        random.seed(args.seed)

    if args.dry_run:
        for _ in range(5):
            log.info(json.dumps(random_doc()))
        return

    if not args.topic:
        ap.error("--topic is required (or use --dry-run)")

    class Stats:
        def __init__(self) -> None:
            self.delivered = 0
            self.errored = 0
            self._warned = 0

        def on_delivery(self, err, msg) -> None:
            if err:
                self.errored += 1
                if self._warned < 5:
                    log.warning("delivery failed: %s", err)
                    self._warned += 1
            else:
                self.delivered += 1

    stats = Stats()
    producer = Producer(
        {
            "bootstrap.servers": args.brokers,
            "enable.idempotence": True,
            "acks": "all",
            "socket.timeout.ms": 10_000,
        }
    )

    stop = False

    def _handle_signal(signum, _frame) -> None:
        nonlocal stop
        stop = True
        log.info("signal %s: finishing in-flight docs...", signum)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    t0 = time.monotonic()
    next_slot = t0
    next_report = t0 + 5.0
    produced = 0

    log.info(
        "topic=%s brokers=%s rate=%s/s count=%s duration=%ss",
        args.topic,
        args.brokers,
        args.rate if args.rate > 0 else "inf",
        args.count or "inf",
        args.duration or "inf",
    )

    try:
        while not stop:
            payload = json.dumps(random_doc()).encode()
            producer.produce(args.topic, value=payload, on_delivery=stats.on_delivery)
            if len(producer) > 10_000:  # backpressure: local queue full
                producer.poll(1.0)
            producer.poll(0)
            produced += 1
            if args.count and produced >= args.count:
                break
            if args.duration and time.monotonic() - t0 >= args.duration:
                break
            if interval:
                now = time.monotonic()
                if now < next_slot:
                    time.sleep(next_slot - now)
                next_slot = max(next_slot, now) + interval
            now = time.monotonic()
            if now >= next_report:
                log.info(
                    "produced=%d delivered=%d errors=%d (current %.0f docs/s)",
                    produced,
                    stats.delivered,
                    stats.errored,
                    produced / (now - t0),
                )
                next_report = now + 5.0
    finally:
        producer.flush(30)
        elapsed = max(time.monotonic() - t0, 1e-9)
        log.info(
            "done: produced=%d delivered=%d errors=%d in %.1fs (avg %.0f docs/s)",
            produced,
            stats.delivered,
            stats.errored,
            elapsed,
            produced / elapsed,
        )


if __name__ == "__main__":
    main()
