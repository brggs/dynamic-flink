from __future__ import annotations

import logging
import signal
import sys
import time

from generator.clock import SimulationClock
from generator.config import Config
from generator.kafka_producer import EventProducer
from generator.scenarios import TrafficSimulator
from generator.schema import validate_event_shape

LOGGER = logging.getLogger(__name__)


class _ShutdownFlag:
    def __init__(self) -> None:
        self.stop = False

    def request(self, *_args: object) -> None:
        self.stop = True


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def main() -> int:
    config = Config.from_env()
    config.validate()
    _setup_logging(config.log_level)

    shutdown = _ShutdownFlag()
    signal.signal(signal.SIGINT, shutdown.request)
    signal.signal(signal.SIGTERM, shutdown.request)

    clock = SimulationClock(config.start_time, config.time_compression)
    simulator = TrafficSimulator(config)
    producer = EventProducer(config)

    emitted = 0
    loop_started = time.monotonic()
    next_tick = time.monotonic()
    last_stats = loop_started

    LOGGER.info(
        "Starting generator (dry_run=%s, topic=%s, brokers=%s, event_rate=%s/s, compression=%sx)",
        config.dry_run,
        config.kafka_topic,
        config.kafka_bootstrap_servers,
        config.event_rate_per_sec,
        config.time_compression,
    )

    try:
        while not shutdown.stop:
            sim_now = clock.now()
            events = simulator.simulate_tick(sim_now)

            for event in events:
                validate_event_shape(event)
                producer.send(event)
                emitted += 1

                if config.max_events is not None and emitted >= config.max_events:
                    shutdown.stop = True
                    break

            now = time.monotonic()
            if now - last_stats >= 5.0:
                elapsed = max(0.001, now - loop_started)
                LOGGER.info(
                    "stats emitted=%d delivered=%d failed=%d rate=%.1f/s active_incidents=%d drift=%s burst=%s queue=%d",
                    emitted,
                    producer.delivered,
                    producer.failed,
                    emitted / elapsed,
                    len(simulator.active_incidents),
                    simulator.active_drift is not None,
                    simulator.active_burst is not None,
                    len(simulator.sequence_queue),
                )
                last_stats = now

            next_tick += config.tick_ms / 1000.0
            sleep_for = max(0.0, next_tick - time.monotonic())
            if sleep_for > 0:
                time.sleep(sleep_for)
    except Exception:
        LOGGER.exception("Generator crashed")
        producer.flush(timeout=10.0)
        return 1

    LOGGER.info("Stopping generator. Flushing producer...")
    producer.flush(timeout=10.0)
    LOGGER.info("Stopped. emitted=%d delivered=%d failed=%d", emitted, producer.delivered, producer.failed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
