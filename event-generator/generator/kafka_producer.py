from __future__ import annotations

import json
import logging
from typing import Optional

from generator.config import Config

LOGGER = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._delivered = 0
        self._failed = 0

        self._dry_run = config.dry_run
        self._producer = None

        if self._dry_run:
            LOGGER.info("Starting in dry-run mode. Kafka publishes are disabled.")
            return

        try:
            from confluent_kafka import Producer
        except Exception as exc:
            raise RuntimeError(
                "confluent-kafka is required unless GEN_DRY_RUN=true"
            ) from exc

        producer_config = {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "client.id": config.kafka_client_id,
            "acks": "1",
        }
        self._producer = Producer(producer_config)

    def send(self, event: dict) -> None:
        if self._dry_run:
            LOGGER.debug("DRY_RUN event=%s", json.dumps(event, separators=(",", ":")))
            self._delivered += 1
            return

        assert self._producer is not None

        key = str(event.get("vehicle_id", ""))
        payload = json.dumps(event, separators=(",", ":")).encode("utf-8")

        while True:
            try:
                self._producer.produce(
                    topic=self.config.kafka_topic,
                    key=key.encode("utf-8"),
                    value=payload,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self._producer.poll(0.05)

        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        if self._dry_run:
            return
        assert self._producer is not None
        self._producer.flush(timeout)

    @property
    def delivered(self) -> int:
        return self._delivered

    @property
    def failed(self) -> int:
        return self._failed

    def _on_delivery(self, err: Optional[BaseException], _msg: object) -> None:
        if err is not None:
            self._failed += 1
            LOGGER.warning("Kafka delivery failed: %s", err)
            return
        self._delivered += 1
