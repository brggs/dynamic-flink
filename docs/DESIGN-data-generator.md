# Design Overview (data generator)

Scope: this document covers the event/data generator used for the initial Docker-based environment. It produces synthetic traffic telemetry for Dynamic Flink demos and tests.

## Purpose

Provide a configurable event stream that is:

- Flavorful (near-future sci-fi smart city theme).
- Realistic enough for stream-processing demonstrations.
- Deterministic when needed (seeded randomness).
- Rich enough to exercise threshold, average-change, and sequence rules.

## Language Recommendation

Use Python 3.12+.

Rationale:

- Required Kafka client is `confluent-kafka` Python.
- Fast iteration for scenario logic and weighted random simulation.
- Good fit for JSON event generation and environment-driven configuration.

## Theme and Naming

The simulated city is a dense near-future megalopolis with mixed ground/aerial transit.

- District naming: `sector-07`, `sector-19`, `arcology-a`, `arcology-f`.
- Sensor naming: `mast-a12`, `loop-g44`, `pad-s03`.
- Road/route naming: `transit-spine-north`, `sky-corridor-3`.
- Vehicle naming: stable IDs such as `veh-g-00291`, `veh-s-01044`, `veh-c-00102`, `veh-t-00078`.

Theme text should appear in docs/logs only. Event field structure stays operational and realistic.

## Event Model

Each output message is one telemetry event from fixed infrastructure.

### Event schema (JSON)

| Field | Type | Notes |
|------|------|------|
| `event_id` | string | UUID or deterministic ID |
| `timestamp` | string (ISO-8601 UTC) | Simulation time (not wall-clock) |
| `sensor_id` | string | Stable infrastructure ID |
| `sensor_type` | enum | `ground_loop`, `aerial_mast`, `spinner_pad` |
| `sector` | string | World zone identifier |
| `zone_mode` | enum | `ground`, `air`, `mixed` |
| `vehicle_id` | string | Stable vehicle ID |
| `vehicle_type` | enum | `ground_car`, `spinner`, `cargo_hauler`, `transit_pod` |
| `speed_kph` | number | Non-negative; may spike during incidents |
| `heading_deg` | integer | 0-359 |
| `occupancy_count` | integer/null | Used where relevant (`transit_pod`, `spinner_pad`) |
| `severity` | enum | `none`, `low`, `medium`, `high`, `critical` |
| `anomaly_flag` | boolean | True when incident logic marks anomaly |
| `anomaly_type` | enum/null | `jam`, `corridor_closure`, `sensor_malfunction`, `low_altitude_incursion`, `implausible_sequence`, `surge` |
| `scenario_id` | string/null | Correlates events created by one scenario |

## Stable Entity Pools

- Vehicles come from prebuilt pools by type and are reused over time.
- Sensors are static and emit repeatedly.
- Route templates define likely sensor sequences per vehicle class.
- Optional seed (`GEN_RANDOM_SEED`) guarantees repeatable runs.

This stability is required for sequence rules and average-change comparisons.

## Simulation Behaviors

### Baseline Flow

- Diurnal pattern over simulation clock:
  - Morning peak
  - Midday normalization
  - Evening peak
  - Quiet overnight
- Per-sector load multipliers (core sectors busier than edge sectors).

### Incident Injection (configurable)

- Traffic jam: speed drops, occupancy rises, localized severity increase.
- Spinner corridor closure: rerouting plus sudden congestion on alternate lanes.
- Sensor malfunction: missing/duplicated/noisy readings from specific sensors.
- Low-altitude incursion: spinner events emitted in ground zones.

### Drift Scenarios

- Gradual worsening over configurable duration.
- Examples: average speed declines and queue occupancy climbs.
- Intended to trigger average-change style rules.

### Burst/Spike Scenarios

- Sudden rate surges at sector or corridor scope.
- Intended to trigger threshold rules.

### Multi-Event Sequence Scenarios

- Generates implausible sensor traversals for selected vehicles.
- Example: impossible cross-sector order or travel time.
- Intended to trigger sequence pattern rules.

## Simulation Clock

The generator uses a simulation clock independent of wall-clock.

- `GEN_TIME_COMPRESSION`: multiplier (for example `20.0` means 1 real second = 20 simulated seconds).
- `GEN_START_TIME`: simulation start timestamp in UTC.
- `GEN_TICK_MS`: loop tick granularity.

All event timestamps use simulation time.

## Kafka Integration

- Client: `confluent-kafka` (`Producer` API).
- Topic: configured by `GEN_KAFKA_TOPIC` (default should match Flink input topic).
- Brokers: `GEN_KAFKA_BOOTSTRAP_SERVERS`.
- Value format: JSON UTF-8.
- Key strategy: default `vehicle_id` to preserve per-vehicle ordering characteristics.
- Delivery mode: async produce with callback metrics and periodic flush.

## Environment Configuration

| Variable | Default | Purpose |
|------|------|------|
| `GEN_KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap servers |
| `GEN_KAFKA_TOPIC` | `events` | Output topic |
| `GEN_EVENT_RATE_PER_SEC` | `50` | Baseline event rate |
| `GEN_TIME_COMPRESSION` | `20.0` | Simulation clock speedup |
| `GEN_INCIDENT_FREQUENCY_PER_MIN` | `0.6` | Incident injection frequency |
| `GEN_DRIFT_ENABLED` | `true` | Enable drift scenarios |
| `GEN_BURST_ENABLED` | `true` | Enable burst scenarios |
| `GEN_SEQUENCE_ENABLED` | `true` | Enable sequence anomalies |
| `GEN_RANDOM_SEED` | unset | Deterministic replay when set |
| `GEN_START_TIME` | `2035-05-01T06:00:00Z` | Simulation initial time |
| `GEN_LOG_LEVEL` | `INFO` | Logging verbosity |

## Proposed Package Layout

```text
event-generator/
  README.md
  requirements.txt
  generator/
    main.py
    config.py
    clock.py
    world.py
    entities.py
    scenarios.py
    kafka_producer.py
    schema.py
```

## README Requirements

The generator README should include:

- World description and naming conventions.
- Full event schema with examples.
- Configuration via environment variables.
- Local run command and Docker run command.
- How each scenario type maps to rule demonstrations.

## Scenario-to-Rule Mapping (Dynamic Flink)

The generator should explicitly support demonstrations of currently implemented rule primitives in `flink-job`.

| Generator scenario | Primary rule/block mapping | Example rule intent |
|------|------|------|
| Baseline flow | `SINGLE_EVENT` | Match a specific vehicle/sensor/sector condition under normal traffic |
| Burst/spike surge | `THRESHOLD` | Alert when event count in window exceeds threshold for a sector/corridor |
| Unique-entity surge | `UNIQUE_THRESHOLD` | Alert when unique `vehicle_id` count crosses threshold in a zone |
| Slow drift congestion | `SIMPLE_ANOMALY` | Alert when latest window count/speed deviation exceeds anomaly threshold |
| Corridor closure after active period | `DROP_TO_ZERO` | Alert when expected flow drops to zero for corridor sensors |
| Implausible movement sequence | Complex rule + `SequenceRuleCondition` | Alert on impossible sensor order/time for the same `vehicle_id` |
| Illegal low-altitude incursion | `SINGLE_EVENT` (high severity) | Match `vehicle_type=spinner` in `zone_mode=ground` with anomaly set |
| Sensor malfunction | `SINGLE_EVENT` or `THRESHOLD` | Match explicit anomaly flags or abnormal repeated/missing emissions |

Implementation notes for mapping:

- Keep `vehicle_id` stable and frequently reused so sequence and grouped window rules are meaningful.
- Include `sector`, `sensor_id`, and `vehicle_type` in every event to support filtering/grouping conditions.
- Emit anomaly metadata (`anomaly_flag`, `anomaly_type`, `severity`) so single-event and threshold-style rules can target incident traffic directly.

## Non-Goals (initial cut)

- Full traffic-physics simulation.
- Geospatial accuracy beyond consistent sector/route abstractions.
- Multi-topic fan-out with independent schemas.

## Acceptance Criteria

- Generator publishes valid events to Kafka continuously.
- Stable IDs are reused across events.
- Time compression works and is observable in timestamps.
- Each scenario type can be enabled/disabled by environment variables.
- A short smoke run demonstrates:
  - normal flow,
  - one drift period,
  - one burst period,
  - one multi-event anomaly sequence.
