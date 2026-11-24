"""Kafka consumer wrapper for one use case (own group, own topic)."""

from __future__ import annotations

import json
import logging

from confluent_kafka import Consumer

log = logging.getLogger(__name__)


# Friendly YAML keys (reference kafka config) -> librdkafka config keys.
_KAFKA_KEY_MAP = {
    "security_protocol": "security.protocol",
    "ssl_certfile": "ssl.certificate.location",
    "ssl_keyfile": "ssl.key.location",
    "ssl_cafile": "ssl.ca.location",
}


def map_kafka_settings(settings: dict | None) -> dict:
    """Translate a YAML kafka sub-block into librdkafka config keys (skips group_id)."""
    return {
        _KAFKA_KEY_MAP.get(k, k): v
        for k, v in (settings or {}).items()
        if k != "group_id" and v is not None
    }


class KafkaEventConsumer:
    def __init__(
        self, bootstrap_servers: str, group: str, topic: str, settings: dict | None = None
    ):
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        conf.update(map_kafka_settings(settings))
        self.topic = topic
        self._consumer = Consumer(conf)
        self._consumer.subscribe([topic])

    def poll(self, timeout: float = 1.0) -> dict | None:
        """Return the next event dict, or None (no message / bad json)."""
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            log.warning("kafka error: %s", msg.error())
            return None
        value = msg.value()
        if value is None:
            return None
        try:
            return json.loads(value)
        except (ValueError, TypeError) as e:
            log.warning("skipping non-JSON message: %s", e)
            return None

    def close(self) -> None:
        self._consumer.close()


class StaticConsumer:
    """Test/in-memory consumer: replays a fixed list of events once."""

    def __init__(self, events: list[dict]):
        self.events = list(events)

    def poll(self, timeout: float = 1.0) -> dict | None:
        return self.events.pop(0) if self.events else None

    def close(self) -> None:
        pass
