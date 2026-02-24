from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Sensor:
    sensor_id: str
    sensor_type: str
    sector: str
    zone_mode: str
    corridor: str


@dataclass(frozen=True)
class Vehicle:
    vehicle_id: str
    vehicle_type: str
    home_sector: str


@dataclass
class ActiveIncident:
    scenario_id: str
    anomaly_type: str
    sector: str
    severity: str
    starts_at: datetime
    ends_at: datetime
    malfunction_sensor_id: Optional[str] = None


@dataclass
class ActiveDrift:
    scenario_id: str
    sector: str
    starts_at: datetime
    ends_at: datetime

    def progress(self, now: datetime) -> float:
        if now <= self.starts_at:
            return 0.0
        if now >= self.ends_at:
            return 1.0
        total = (self.ends_at - self.starts_at).total_seconds()
        if total <= 0:
            return 1.0
        elapsed = (now - self.starts_at).total_seconds()
        return max(0.0, min(1.0, elapsed / total))


@dataclass
class ActiveBurst:
    scenario_id: str
    sector: str
    starts_at: datetime
    ends_at: datetime
    multiplier: float


@dataclass(frozen=True)
class SequenceScheduledEvent:
    scheduled_at: datetime
    event: dict
