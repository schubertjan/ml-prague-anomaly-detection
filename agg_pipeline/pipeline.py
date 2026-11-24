"""Pipeline runtime: consume -> aggregate -> write to ES.

Run one instance per use case config, e.g.:

    python -m agg_pipeline --config configs/syslog.yaml
    python -m agg_pipeline --config configs/access_log.yaml
"""

from __future__ import annotations

import logging
import signal
import threading

from .aggregator import WindowAggregator
from .config import UsecaseConfig
from .consumer import KafkaEventConsumer
from .es_writer import EsWriter

log = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, cfg: UsecaseConfig, consumer=None, writer=None):
        self.cfg = cfg
        self.consumer = consumer or KafkaEventConsumer(
            cfg.kafka_bootstrap_servers, cfg.consumer_group, cfg.topic, cfg.kafka_consumer
        )
        self.writer = writer or EsWriter(cfg.es_hosts, cfg.index_name, cfg.es_settings())
        self.agg = WindowAggregator(cfg)
        self._stop = threading.Event()

    def _handle_signal(self, signum, _frame) -> None:
        log.info("signal %s received, shutting down", signum)
        self._stop.set()

    def run(self, poll_timeout: float = 1.0) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        log.info(
            "use case %r: topic=%s group=%s index=%s freq=%s",
            self.cfg.name,
            self.cfg.topic,
            self.cfg.consumer_group,
            self.cfg.index_name,
            self.cfg.freq,
        )
        try:
            while not self._stop.is_set():
                event = self.consumer.poll(poll_timeout)
                if event is not None:
                    self.agg.process(event)
                self._flush()
        finally:
            # flush in-flight windows so nothing is lost on shutdown
            self._flush(self.agg.flush_all())
            self.consumer.close()
            log.info("use case %r stopped", self.cfg.name)

    def _flush(self, docs: list | None = None) -> None:
        docs = self.agg.flush_expired() if docs is None else docs
        if docs:
            self.writer.write(docs)


def main(argv: list[str] | None = None) -> None:
    import argparse

    p = argparse.ArgumentParser(description="Run one aggregation use case")
    p.add_argument("--config", required=True, help="path to use-case YAML config")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from .config import load_config

    Pipeline(load_config(args.config)).run()
