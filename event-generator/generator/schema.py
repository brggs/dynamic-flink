from __future__ import annotations

from typing import Any, Dict


REQUIRED_FIELDS = {
    "event_id",
    "@timestamp",
    "timestamp",
    "customer",
    "sensor_id",
    "sensor_type",
    "sector",
    "zone_mode",
    "vehicle_id",
    "vehicle_type",
    "speed_kph",
    "heading_deg",
    "severity",
    "anomaly_flag",
}


def validate_event_shape(event: Dict[str, Any]) -> None:
    missing = REQUIRED_FIELDS.difference(event.keys())
    if missing:
        raise ValueError(f"Event missing required fields: {sorted(missing)}")

    if not isinstance(event["event_id"], str) or not event["event_id"]:
        raise ValueError("event_id must be a non-empty string")

    if not isinstance(event["vehicle_id"], str) or not event["vehicle_id"]:
        raise ValueError("vehicle_id must be a non-empty string")

    if not isinstance(event["sensor_id"], str) or not event["sensor_id"]:
        raise ValueError("sensor_id must be a non-empty string")

    speed = event["speed_kph"]
    if not isinstance(speed, (int, float)) or speed < 0:
        raise ValueError("speed_kph must be a non-negative number")

    heading = event["heading_deg"]
    if not isinstance(heading, int) or heading < 0 or heading > 359:
        raise ValueError("heading_deg must be an integer from 0 to 359")
