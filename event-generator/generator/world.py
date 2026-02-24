from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Dict, List

from generator.entities import Sensor, Vehicle


@dataclass(frozen=True)
class World:
    city_name: str
    sectors: List[str]
    sensors: List[Sensor]
    sensors_by_sector: Dict[str, List[Sensor]]
    sensors_by_type: Dict[str, List[Sensor]]
    vehicles: List[Vehicle]
    vehicles_by_type: Dict[str, List[Vehicle]]

    def pick_sensor_for_vehicle(self, rng: random.Random, vehicle_type: str, sector: str) -> Sensor:
        candidates = self.sensors_by_sector.get(sector, [])
        if not candidates:
            candidates = self.sensors

        if vehicle_type in {"ground_car", "cargo_hauler"}:
            preferred = [s for s in candidates if s.sensor_type == "ground_loop"]
        elif vehicle_type == "spinner":
            preferred = [s for s in candidates if s.sensor_type in {"aerial_mast", "spinner_pad"}]
        else:
            preferred = [s for s in candidates if s.sensor_type in {"spinner_pad", "ground_loop"}]

        return rng.choice(preferred or candidates)


def build_default_world(rng: random.Random) -> World:
    sectors = [
        "sector-03",
        "sector-07",
        "sector-11",
        "sector-19",
        "arcology-a",
        "arcology-f",
        "arcology-k",
        "dock-ring-2",
    ]

    corridors = [
        "transit-spine-north",
        "transit-spine-south",
        "sky-corridor-1",
        "sky-corridor-3",
    ]

    sensors: List[Sensor] = []
    for i, sector in enumerate(sectors):
        corridor = corridors[i % len(corridors)]
        sector_token = sector.replace("-", "")

        for n in range(1, 4):
            sensors.append(
                Sensor(
                    sensor_id=f"loop-{sector_token}-{n:02d}",
                    sensor_type="ground_loop",
                    sector=sector,
                    zone_mode="ground",
                    corridor=corridor,
                )
            )

        for n in range(1, 3):
            sensors.append(
                Sensor(
                    sensor_id=f"mast-{sector_token}-{n:02d}",
                    sensor_type="aerial_mast",
                    sector=sector,
                    zone_mode="air",
                    corridor=corridor,
                )
            )

        sensors.append(
            Sensor(
                sensor_id=f"pad-{sector_token}-01",
                sensor_type="spinner_pad",
                sector=sector,
                zone_mode="mixed",
                corridor=corridor,
            )
        )

    sensors_by_sector: Dict[str, List[Sensor]] = {sector: [] for sector in sectors}
    sensors_by_type: Dict[str, List[Sensor]] = {
        "ground_loop": [],
        "aerial_mast": [],
        "spinner_pad": [],
    }

    for sensor in sensors:
        sensors_by_sector[sensor.sector].append(sensor)
        sensors_by_type[sensor.sensor_type].append(sensor)

    vehicles_by_type: Dict[str, List[Vehicle]] = {
        "ground_car": [],
        "spinner": [],
        "cargo_hauler": [],
        "transit_pod": [],
    }

    pool_sizes = {
        "ground_car": 260,
        "spinner": 90,
        "cargo_hauler": 36,
        "transit_pod": 64,
    }

    prefixes = {
        "ground_car": "g",
        "spinner": "s",
        "cargo_hauler": "c",
        "transit_pod": "t",
    }

    for vehicle_type, size in pool_sizes.items():
        prefix = prefixes[vehicle_type]
        for idx in range(1, size + 1):
            vehicles_by_type[vehicle_type].append(
                Vehicle(
                    vehicle_id=f"veh-{prefix}-{idx:05d}",
                    vehicle_type=vehicle_type,
                    home_sector=rng.choice(sectors),
                )
            )

    vehicles: List[Vehicle] = []
    for vehicle_list in vehicles_by_type.values():
        vehicles.extend(vehicle_list)

    return World(
        city_name="Astra Megapolis",
        sectors=sectors,
        sensors=sensors,
        sensors_by_sector=sensors_by_sector,
        sensors_by_type=sensors_by_type,
        vehicles=vehicles,
        vehicles_by_type=vehicles_by_type,
    )
