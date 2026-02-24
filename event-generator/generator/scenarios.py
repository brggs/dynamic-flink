from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from generator.config import Config
from generator.entities import (
    ActiveBurst,
    ActiveDrift,
    ActiveIncident,
    SequenceScheduledEvent,
    Sensor,
    Vehicle,
)
from generator.world import World, build_default_world

VEHICLE_TYPE_WEIGHTS: Dict[str, float] = {
    "ground_car": 0.62,
    "spinner": 0.18,
    "cargo_hauler": 0.08,
    "transit_pod": 0.12,
}

SECTOR_WEIGHTS: Dict[str, float] = {
    "sector-03": 1.1,
    "sector-07": 1.45,
    "sector-11": 1.2,
    "sector-19": 1.35,
    "arcology-a": 1.3,
    "arcology-f": 1.0,
    "arcology-k": 0.9,
    "dock-ring-2": 0.75,
}


class TrafficSimulator:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.rng = random.Random(config.random_seed)
        self.world: World = build_default_world(self.rng)

        self._scenario_counter = 0
        self._event_counter = 0

        self.active_incidents: List[ActiveIncident] = []
        self.active_drift: Optional[ActiveDrift] = None
        self.active_burst: Optional[ActiveBurst] = None
        self.sequence_queue: List[SequenceScheduledEvent] = []

    def simulate_tick(self, sim_now: datetime) -> List[dict]:
        self._expire_scenarios(sim_now)
        self._maybe_start_incident(sim_now)
        self._maybe_start_drift(sim_now)
        self._maybe_start_burst(sim_now)
        self._maybe_start_sequence(sim_now)

        events: List[dict] = []
        drift_progress = self.active_drift.progress(sim_now) if self.active_drift else 0.0

        base_rate = self.config.event_rate_per_sec
        tick_real_seconds = self.config.tick_ms / 1000.0
        diurnal = self._diurnal_multiplier(sim_now)

        expected_base = base_rate * tick_real_seconds * diurnal
        for _ in range(self._stochastic_count(expected_base)):
            events.append(self._generate_event(sim_now, drift_progress=drift_progress))

        if self.active_burst:
            extra_expected = base_rate * tick_real_seconds * max(0.0, self.active_burst.multiplier - 1.0) * 0.65
            for _ in range(self._stochastic_count(extra_expected)):
                events.append(
                    self._generate_event(
                        sim_now,
                        force_sector=self.active_burst.sector,
                        anomaly_type="surge",
                        severity="medium",
                        scenario_id=self.active_burst.scenario_id,
                        drift_progress=drift_progress,
                    )
                )

        events.extend(self._emit_due_sequence_events(sim_now))
        return events

    def _stochastic_count(self, expected: float) -> int:
        if expected <= 0:
            return 0
        whole = int(expected)
        frac = expected - whole
        if self.rng.random() < frac:
            whole += 1
        return whole

    def _expire_scenarios(self, now: datetime) -> None:
        self.active_incidents = [i for i in self.active_incidents if i.ends_at > now]

        if self.active_drift and self.active_drift.ends_at <= now:
            self.active_drift = None

        if self.active_burst and self.active_burst.ends_at <= now:
            self.active_burst = None

    def _maybe_start_incident(self, now: datetime) -> None:
        if len(self.active_incidents) >= 4:
            return

        tick_sim_seconds = (self.config.tick_ms / 1000.0) * self.config.time_compression
        p = self.config.incident_frequency_per_min * (tick_sim_seconds / 60.0)
        if self.rng.random() >= p:
            return

        anomaly_type = self.rng.choice([
            "jam",
            "corridor_closure",
            "sensor_malfunction",
            "low_altitude_incursion",
        ])
        sector = self._pick_sector(None)
        duration = self.rng.randint(
            self.config.incident_min_duration_sec,
            self.config.incident_max_duration_sec,
        )
        scenario_id = self._next_scenario_id("incident")

        malfunction_sensor_id = None
        if anomaly_type == "sensor_malfunction":
            malfunction_sensor_id = self.rng.choice(self.world.sensors_by_sector[sector]).sensor_id

        severity = {
            "jam": "medium",
            "corridor_closure": "high",
            "sensor_malfunction": "medium",
            "low_altitude_incursion": "critical",
        }[anomaly_type]

        self.active_incidents.append(
            ActiveIncident(
                scenario_id=scenario_id,
                anomaly_type=anomaly_type,
                sector=sector,
                severity=severity,
                starts_at=now,
                ends_at=now + timedelta(seconds=duration),
                malfunction_sensor_id=malfunction_sensor_id,
            )
        )

    def _maybe_start_drift(self, now: datetime) -> None:
        if not self.config.drift_enabled or self.active_drift is not None:
            return

        tick_sim_seconds = (self.config.tick_ms / 1000.0) * self.config.time_compression
        p = self.config.drift_frequency_per_hour * (tick_sim_seconds / 3600.0)
        if self.rng.random() >= p:
            return

        duration = self.config.drift_duration_sec
        self.active_drift = ActiveDrift(
            scenario_id=self._next_scenario_id("drift"),
            sector=self._pick_sector(None),
            starts_at=now,
            ends_at=now + timedelta(seconds=duration),
        )

    def _maybe_start_burst(self, now: datetime) -> None:
        if not self.config.burst_enabled or self.active_burst is not None:
            return

        tick_sim_seconds = (self.config.tick_ms / 1000.0) * self.config.time_compression
        p = self.config.burst_frequency_per_hour * (tick_sim_seconds / 3600.0)
        if self.rng.random() >= p:
            return

        self.active_burst = ActiveBurst(
            scenario_id=self._next_scenario_id("burst"),
            sector=self._pick_sector(None),
            starts_at=now,
            ends_at=now + timedelta(seconds=self.config.burst_duration_sec),
            multiplier=self.config.burst_multiplier,
        )

    def _maybe_start_sequence(self, now: datetime) -> None:
        if not self.config.sequence_enabled:
            return

        tick_sim_seconds = (self.config.tick_ms / 1000.0) * self.config.time_compression
        p = self.config.sequence_frequency_per_min * (tick_sim_seconds / 60.0)
        if self.rng.random() >= p:
            return

        scenario_id = self._next_scenario_id("sequence")
        vehicle = self._pick_vehicle("spinner")

        sectors = self.rng.sample(self.world.sectors, 4)
        offsets = [0, 3, 7, 11]

        for idx, sector in enumerate(sectors):
            sensor = self.world.pick_sensor_for_vehicle(self.rng, vehicle.vehicle_type, sector)
            event = self._build_event(
                sim_now=now + timedelta(seconds=offsets[idx]),
                vehicle=vehicle,
                sensor=sensor,
                anomaly_type="implausible_sequence",
                severity="high",
                scenario_id=scenario_id,
                speed_override=self.rng.uniform(180, 280),
            )
            self.sequence_queue.append(
                SequenceScheduledEvent(scheduled_at=now + timedelta(seconds=offsets[idx]), event=event)
            )

        self.sequence_queue.sort(key=lambda item: item.scheduled_at)

    def _emit_due_sequence_events(self, now: datetime) -> List[dict]:
        ready: List[dict] = []
        while self.sequence_queue and self.sequence_queue[0].scheduled_at <= now:
            ready.append(self.sequence_queue.pop(0).event)
        return ready

    def _generate_event(
        self,
        sim_now: datetime,
        force_sector: Optional[str] = None,
        anomaly_type: Optional[str] = None,
        severity: str = "none",
        scenario_id: Optional[str] = None,
        drift_progress: float = 0.0,
    ) -> dict:
        if anomaly_type is None and self.active_incidents and self.rng.random() < 0.24:
            incident = self.rng.choice(self.active_incidents)
            return self._generate_incident_event(sim_now, incident, drift_progress)

        vehicle_type = self._weighted_choice(VEHICLE_TYPE_WEIGHTS)
        vehicle = self._pick_vehicle(vehicle_type)
        sector = force_sector or self._pick_sector(vehicle.home_sector)
        sensor = self.world.pick_sensor_for_vehicle(self.rng, vehicle.vehicle_type, sector)

        event = self._build_event(
            sim_now=sim_now,
            vehicle=vehicle,
            sensor=sensor,
            anomaly_type=anomaly_type,
            severity=severity,
            scenario_id=scenario_id,
            drift_progress=drift_progress,
        )
        return event

    def _generate_incident_event(self, sim_now: datetime, incident: ActiveIncident, drift_progress: float) -> dict:
        if incident.anomaly_type == "low_altitude_incursion":
            vehicle = self._pick_vehicle("spinner")
            ground_sensors = [
                s for s in self.world.sensors_by_sector[incident.sector] if s.sensor_type == "ground_loop"
            ]
            sensor = self.rng.choice(ground_sensors or self.world.sensors_by_sector[incident.sector])
            return self._build_event(
                sim_now=sim_now,
                vehicle=vehicle,
                sensor=sensor,
                anomaly_type=incident.anomaly_type,
                severity=incident.severity,
                scenario_id=incident.scenario_id,
                speed_override=self.rng.uniform(80, 180),
                drift_progress=drift_progress,
            )

        if incident.anomaly_type == "sensor_malfunction" and incident.malfunction_sensor_id:
            sensor = next(
                s for s in self.world.sensors if s.sensor_id == incident.malfunction_sensor_id
            )
            vehicle = self._pick_vehicle(self._weighted_choice(VEHICLE_TYPE_WEIGHTS))
            return self._build_event(
                sim_now=sim_now,
                vehicle=vehicle,
                sensor=sensor,
                anomaly_type=incident.anomaly_type,
                severity=incident.severity,
                scenario_id=incident.scenario_id,
                speed_override=self.rng.uniform(0, 220),
                drift_progress=drift_progress,
            )

        if incident.anomaly_type == "jam":
            vehicle_type = self.rng.choice(["ground_car", "cargo_hauler", "transit_pod"])
            vehicle = self._pick_vehicle(vehicle_type)
            sensor = self.world.pick_sensor_for_vehicle(self.rng, vehicle_type, incident.sector)
            return self._build_event(
                sim_now=sim_now,
                vehicle=vehicle,
                sensor=sensor,
                anomaly_type=incident.anomaly_type,
                severity=incident.severity,
                scenario_id=incident.scenario_id,
                speed_override=self.rng.uniform(3, 24),
                drift_progress=drift_progress,
            )

        vehicle = self._pick_vehicle(self._weighted_choice(VEHICLE_TYPE_WEIGHTS))
        sensor = self.world.pick_sensor_for_vehicle(self.rng, vehicle.vehicle_type, incident.sector)
        return self._build_event(
            sim_now=sim_now,
            vehicle=vehicle,
            sensor=sensor,
            anomaly_type=incident.anomaly_type,
            severity=incident.severity,
            scenario_id=incident.scenario_id,
            speed_override=self.rng.uniform(8, 45),
            drift_progress=drift_progress,
        )

    def _build_event(
        self,
        sim_now: datetime,
        vehicle: Vehicle,
        sensor: Sensor,
        anomaly_type: Optional[str],
        severity: str,
        scenario_id: Optional[str],
        speed_override: Optional[float] = None,
        drift_progress: float = 0.0,
    ) -> dict:
        speed = speed_override if speed_override is not None else self._base_speed(vehicle.vehicle_type)

        if self.active_drift and sensor.sector == self.active_drift.sector:
            drift_penalty = 0.35 * drift_progress
            speed = max(2.0, speed * (1.0 - drift_penalty))

        heading = self.rng.randint(0, 359)
        occupancy = self._occupancy_count(vehicle.vehicle_type, sensor.sensor_type, anomaly_type)

        self._event_counter += 1
        is_anomaly = anomaly_type is not None and anomaly_type != ""

        if not is_anomaly:
            severity = "none"

        event_time = sim_now.astimezone(timezone.utc)

        return {
            "event_id": f"evt-{self._event_counter:012d}",
            "@timestamp": event_time.isoformat().replace("+00:00", "Z"),
            "timestamp": event_time.isoformat().replace("+00:00", "Z"),
            "customer": self.config.customer,
            "city": self.world.city_name,
            "sensor_id": sensor.sensor_id,
            "sensor_type": sensor.sensor_type,
            "sector": sensor.sector,
            "zone_mode": sensor.zone_mode,
            "corridor": sensor.corridor,
            "vehicle_id": vehicle.vehicle_id,
            "vehicle_type": vehicle.vehicle_type,
            "speed_kph": round(speed, 2),
            "heading_deg": heading,
            "occupancy_count": occupancy,
            "severity": severity,
            "anomaly_flag": is_anomaly,
            "anomaly_type": anomaly_type,
            "scenario_id": scenario_id,
        }

    def _base_speed(self, vehicle_type: str) -> float:
        if vehicle_type == "ground_car":
            return self.rng.uniform(35, 115)
        if vehicle_type == "spinner":
            return self.rng.uniform(120, 270)
        if vehicle_type == "cargo_hauler":
            return self.rng.uniform(28, 92)
        return self.rng.uniform(45, 140)

    def _occupancy_count(
        self,
        vehicle_type: str,
        sensor_type: str,
        anomaly_type: Optional[str],
    ) -> Optional[int]:
        if vehicle_type == "transit_pod":
            if anomaly_type == "jam":
                return self.rng.randint(35, 95)
            return self.rng.randint(8, 64)

        if sensor_type == "spinner_pad":
            return self.rng.randint(0, 12)

        return None

    def _pick_vehicle(self, vehicle_type: str) -> Vehicle:
        return self.rng.choice(self.world.vehicles_by_type[vehicle_type])

    def _pick_sector(self, preferred: Optional[str]) -> str:
        sectors = self.world.sectors
        weights: List[float] = []
        for sector in sectors:
            weight = SECTOR_WEIGHTS.get(sector, 1.0)
            if preferred and sector == preferred:
                weight *= 1.6
            weights.append(weight)
        return self.rng.choices(sectors, weights=weights, k=1)[0]

    def _weighted_choice(self, weights: Dict[str, float]) -> str:
        keys = list(weights.keys())
        values = [weights[k] for k in keys]
        return self.rng.choices(keys, weights=values, k=1)[0]

    def _diurnal_multiplier(self, now: datetime) -> float:
        hour = now.hour
        if 6 <= hour < 10:
            base = 1.9
        elif 10 <= hour < 16:
            base = 1.15
        elif 16 <= hour < 20:
            base = 2.0
        elif 20 <= hour < 23:
            base = 0.95
        else:
            base = 0.4

        return max(0.1, base + self.rng.uniform(-0.08, 0.08))

    def _next_scenario_id(self, prefix: str) -> str:
        self._scenario_counter += 1
        return f"{prefix}-{self._scenario_counter:06d}"
