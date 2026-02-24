# Event Generator (Sci-Fi Smart City)

This generator produces synthetic traffic telemetry for Dynamic Flink demos in a near-future megalopolis.

The world theme is intentionally atmospheric (sectors, arcologies, sky corridors), while event fields stay practical for stream processing.

## World Model

- City: `Astra Megapolis`
- Zones: sectors and arcologies such as `sector-07`, `arcology-a`, `dock-ring-2`
- Sensors:
  - `ground_loop` (ground lanes)
  - `aerial_mast` (spinner corridors)
  - `spinner_pad` (mixed transfer points)
- Vehicle types:
  - `ground_car`
  - `spinner`
  - `cargo_hauler`
  - `transit_pod`

Vehicle IDs and sensor IDs are stable and reused across events so sequence and rolling-window rules can match meaningfully.

## Event Schema

Each event is JSON and includes:

- `event_id`
- `@timestamp` (simulation timestamp, ISO-8601 UTC)
- `timestamp` (same as `@timestamp` for convenience)
- `customer`
- `city`
- `sensor_id`
- `sensor_type`
- `sector`
- `zone_mode`
- `corridor`
- `vehicle_id`
- `vehicle_type`
- `speed_kph`
- `heading_deg`
- `occupancy_count` (or `null`)
- `severity` (`none|low|medium|high|critical`)
- `anomaly_flag`
- `anomaly_type` (`jam|corridor_closure|sensor_malfunction|low_altitude_incursion|implausible_sequence|surge|null`)
- `scenario_id` (or `null`)

Example event:

```json
{
  "event_id": "evt-000000000123",
  "@timestamp": "2035-05-01T06:01:44.000000Z",
  "timestamp": "2035-05-01T06:01:44.000000Z",
  "customer": "neon-transit-authority",
  "city": "Astra Megapolis",
  "sensor_id": "loop-sector-07-01",
  "sensor_type": "ground_loop",
  "sector": "sector-07",
  "zone_mode": "ground",
  "corridor": "transit-spine-north",
  "vehicle_id": "veh-g-00091",
  "vehicle_type": "ground_car",
  "speed_kph": 48.12,
  "heading_deg": 273,
  "occupancy_count": null,
  "severity": "none",
  "anomaly_flag": false,
  "anomaly_type": null,
  "scenario_id": null
}
```

## Simulation Behaviors

- Normal diurnal flow with morning/evening peaks and quiet overnight periods.
- Incident injection:
  - traffic jams
  - spinner corridor closures
  - sensor malfunctions
  - illegal low-altitude spinner incursions into ground zones
- Slow drift scenarios for average-change style detection.
- Burst/surge scenarios for threshold detection.
- Implausible multi-zone sequence scenarios for sequence-rule detection.

## Mapping to Dynamic Flink Rules

- Baseline filtering: `SINGLE_EVENT`
- Spike/burst count: `THRESHOLD`
- Unique vehicle surge: `UNIQUE_THRESHOLD`
- Gradual worsening: `SIMPLE_ANOMALY`
- Flow collapse after active period: `DROP_TO_ZERO`
- Impossible traversal order/time: complex rules using `SequenceRuleCondition`

## Configuration

All behavior is controlled by environment variables.

| Variable | Default | Description |
|---|---|---|
| `GEN_KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap servers |
| `GEN_KAFKA_TOPIC` | `events` | Kafka topic |
| `GEN_KAFKA_CLIENT_ID` | `dynamic-flink-event-generator` | Kafka client id |
| `GEN_CUSTOMER` | `neon-transit-authority` | Event customer field |
| `GEN_EVENT_RATE_PER_SEC` | `50` | Baseline event emission rate |
| `GEN_TICK_MS` | `200` | Main loop tick interval (wall time) |
| `GEN_TIME_COMPRESSION` | `20.0` | Simulation time speedup |
| `GEN_INCIDENT_FREQUENCY_PER_MIN` | `0.6` | Incident starts per simulated minute (expected) |
| `GEN_INCIDENT_MIN_DURATION_SEC` | `120` | Min incident duration (sim seconds) |
| `GEN_INCIDENT_MAX_DURATION_SEC` | `600` | Max incident duration (sim seconds) |
| `GEN_DRIFT_ENABLED` | `true` | Enable drift scenarios |
| `GEN_DRIFT_FREQUENCY_PER_HOUR` | `1.0` | Drift starts per simulated hour (expected) |
| `GEN_DRIFT_DURATION_SEC` | `900` | Drift duration (sim seconds) |
| `GEN_BURST_ENABLED` | `true` | Enable burst scenarios |
| `GEN_BURST_FREQUENCY_PER_HOUR` | `1.2` | Burst starts per simulated hour (expected) |
| `GEN_BURST_DURATION_SEC` | `240` | Burst duration (sim seconds) |
| `GEN_BURST_MULTIPLIER` | `3.0` | Extra event-rate multiplier during burst |
| `GEN_SEQUENCE_ENABLED` | `true` | Enable sequence anomalies |
| `GEN_SEQUENCE_FREQUENCY_PER_MIN` | `0.2` | Sequence scenario starts per simulated minute (expected) |
| `GEN_RANDOM_SEED` | unset | Set for deterministic replay |
| `GEN_START_TIME` | `2035-05-01T06:00:00Z` | Simulation start time |
| `GEN_LOG_LEVEL` | `INFO` | Log level |
| `GEN_DRY_RUN` | `false` | If true, skip Kafka and only log events/stats |
| `GEN_MAX_EVENTS` | unset | Stop automatically after N events |

## Run Locally

Prerequisites:

- Python 3.12+
- Kafka reachable from this process

Install dependencies:

```bash
cd event-generator
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run (Kafka publish):

```bash
python -m generator.main
```

Run dry mode (no Kafka needed):

```bash
GEN_DRY_RUN=true GEN_MAX_EVENTS=200 python -m generator.main
```

## Run in Docker

Build:

```bash
docker build -t dynamic-flink-event-generator:latest event-generator
```

Run:

```bash
docker run --rm \
  -e GEN_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092 \
  -e GEN_KAFKA_TOPIC=events \
  dynamic-flink-event-generator:latest
```

## Notes

- `@timestamp` is included for compatibility with the Flink job input timestamp extraction.
- Kafka key defaults to `vehicle_id`, improving per-vehicle ordering characteristics.
