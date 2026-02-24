from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


def _parse_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _parse_optional_int(value: str) -> Optional[int]:
    if value is None or value.strip() == "":
        return None
    return int(value)


def _parse_start_time(raw: str) -> datetime:
    if raw is None or raw.strip() == "":
        return datetime(2035, 5, 1, 6, 0, 0, tzinfo=timezone.utc)
    value = raw.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_client_id: str
    customer: str

    event_rate_per_sec: float
    tick_ms: int
    time_compression: float

    incident_frequency_per_min: float
    incident_min_duration_sec: int
    incident_max_duration_sec: int

    drift_enabled: bool
    drift_frequency_per_hour: float
    drift_duration_sec: int

    burst_enabled: bool
    burst_frequency_per_hour: float
    burst_duration_sec: int
    burst_multiplier: float

    sequence_enabled: bool
    sequence_frequency_per_min: float

    random_seed: Optional[int]
    start_time: datetime

    log_level: str
    dry_run: bool
    max_events: Optional[int]

    @staticmethod
    def from_env() -> "Config":
        return Config(
            kafka_bootstrap_servers=os.getenv("GEN_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_topic=os.getenv("GEN_KAFKA_TOPIC", "events"),
            kafka_client_id=os.getenv("GEN_KAFKA_CLIENT_ID", "dynamic-flink-event-generator"),
            customer=os.getenv("GEN_CUSTOMER", "neon-transit-authority"),
            event_rate_per_sec=float(os.getenv("GEN_EVENT_RATE_PER_SEC", "50")),
            tick_ms=int(os.getenv("GEN_TICK_MS", "200")),
            time_compression=float(os.getenv("GEN_TIME_COMPRESSION", "20.0")),
            incident_frequency_per_min=float(os.getenv("GEN_INCIDENT_FREQUENCY_PER_MIN", "0.6")),
            incident_min_duration_sec=int(os.getenv("GEN_INCIDENT_MIN_DURATION_SEC", "120")),
            incident_max_duration_sec=int(os.getenv("GEN_INCIDENT_MAX_DURATION_SEC", "600")),
            drift_enabled=_parse_bool(os.getenv("GEN_DRIFT_ENABLED"), True),
            drift_frequency_per_hour=float(os.getenv("GEN_DRIFT_FREQUENCY_PER_HOUR", "1.0")),
            drift_duration_sec=int(os.getenv("GEN_DRIFT_DURATION_SEC", "900")),
            burst_enabled=_parse_bool(os.getenv("GEN_BURST_ENABLED"), True),
            burst_frequency_per_hour=float(os.getenv("GEN_BURST_FREQUENCY_PER_HOUR", "1.2")),
            burst_duration_sec=int(os.getenv("GEN_BURST_DURATION_SEC", "240")),
            burst_multiplier=float(os.getenv("GEN_BURST_MULTIPLIER", "3.0")),
            sequence_enabled=_parse_bool(os.getenv("GEN_SEQUENCE_ENABLED"), True),
            sequence_frequency_per_min=float(os.getenv("GEN_SEQUENCE_FREQUENCY_PER_MIN", "0.2")),
            random_seed=_parse_optional_int(os.getenv("GEN_RANDOM_SEED")),
            start_time=_parse_start_time(os.getenv("GEN_START_TIME")),
            log_level=os.getenv("GEN_LOG_LEVEL", "INFO").upper(),
            dry_run=_parse_bool(os.getenv("GEN_DRY_RUN"), False),
            max_events=_parse_optional_int(os.getenv("GEN_MAX_EVENTS")),
        )

    def validate(self) -> None:
        if self.event_rate_per_sec <= 0:
            raise ValueError("GEN_EVENT_RATE_PER_SEC must be > 0")
        if self.tick_ms <= 0:
            raise ValueError("GEN_TICK_MS must be > 0")
        if self.time_compression <= 0:
            raise ValueError("GEN_TIME_COMPRESSION must be > 0")
        if self.incident_frequency_per_min < 0:
            raise ValueError("GEN_INCIDENT_FREQUENCY_PER_MIN must be >= 0")
        if self.incident_min_duration_sec <= 0 or self.incident_max_duration_sec <= 0:
            raise ValueError("Incident durations must be > 0")
        if self.incident_max_duration_sec < self.incident_min_duration_sec:
            raise ValueError("GEN_INCIDENT_MAX_DURATION_SEC must be >= GEN_INCIDENT_MIN_DURATION_SEC")
        if self.drift_frequency_per_hour < 0 or self.burst_frequency_per_hour < 0:
            raise ValueError("Drift/burst frequencies must be >= 0")
        if self.drift_duration_sec <= 0 or self.burst_duration_sec <= 0:
            raise ValueError("Drift/burst durations must be > 0")
        if self.burst_multiplier < 1.0:
            raise ValueError("GEN_BURST_MULTIPLIER must be >= 1.0")
        if self.sequence_frequency_per_min < 0:
            raise ValueError("GEN_SEQUENCE_FREQUENCY_PER_MIN must be >= 0")
