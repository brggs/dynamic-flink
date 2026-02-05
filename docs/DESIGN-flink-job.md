# Design Overview (flink-job module)

Scope: this document covers the `flink-job` subproject only. If/when other components are added (e.g., control plane, UI, services), add sibling design docs or a top-level architecture overview to describe interactions across modules.

## Purpose
Streaming job that ingests control messages (rules) and events, evaluates them against configurable blocks (single event, thresholds, unique thresholds, anomalies, drop-to-zero), and emits matched output events.

## High-level flow
1) **Sources**
   - Control input: broadcast stream of `ControlInput` messages (add/update/remove rules).
   - Event input: main data stream of raw events (JSON strings).
2) **Broadcast process**
   - `InputBroadcastProcessFunction` keeps rules in broadcast state, parses incoming events (`InputEvent`), and routes matches via side outputs keyed by block type.
3) **Blocks / processors**
   - Implemented under `blocks/*` with processors for each block type; use Flink state and timers to manage windows/thresholds.
4) **Aggregation & grouping**
   - Rules may define `groupByField` and block-level aggregation grouping fields; used to build aggregation keys and keyed state.
5) **Outputs**
   - Matched events serialized via `OutputEventSerializationSchema` (jsoniter), written to sinks (configured downstream).

## Key components
- `InputBroadcastProcessFunction`: core router; maintains broadcast rule state; emits `MatchedEvent` via side outputs.
- `Rule`, `Block`, `RuleCondition`: rule model and validation.
- Block processors: `SingleEventBlockProcessor`, `ThresholdBlockProcessor`, `UniqueThresholdBlockProcessor`, `SimpleAnomalyBlockProcessor`, `DropToZeroBlockProcessor`.
- Test harness: `IntegrationTestCluster` spins up Flink MiniCluster for integration tests.

## Dependencies/pinning notes (as of Feb 2026)
- Flink 1.20.0, Java 17.
- Chill pinned to 0.10.0 (`chill-java`, `chill_2.12`) to keep Kryo serializer compatible with Java 17.
- Jackson (direct) aligned to 2.15.2; most code uses Flink’s shaded Jackson.
- Logging: Log4j2 2.22.1; Log4j 1.x removed.
- jsoniter 0.9.23 used for event serialization/parsing; consider future consolidation to Jackson.

## Testing
- Unit tests: `mvn test -DskipITs`
- Integration tests: `mvn test` (requires ability to open ports for Flink MiniCluster; BLOB port set to `0` for ephemeral).
- Mockito inline mock maker disabled; subclass mock maker in use with `--add-opens java.base/java.util=ALL-UNNAMED`.

## Operational notes
- Keep side-output tags in sync with block types.
- If changing serializers, verify Kryo/Flink type registration impacts and shaded vs. unshaded Jackson usage.
- Ports: MiniCluster uses ephemeral BLOB port; environment must allow binding.
