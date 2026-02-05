# Agents Guide (repo-wide)

Purpose: lightweight norms for folks (and tools/LLM “agents”) working in this repository so changes stay consistent and low-friction.

## Code & style
- Java 17, Flink 1.20.0.
- Logging: Log4j2 (2.22.1) with SLF4J binding; Log4j 1.x removed.
- JSON: Prefer Flink’s shaded Jackson; jsoniter 0.9.23 still used in `flink-job` for event serialization (see DESIGN-flink-job).
- Kryo/Chill: Chill pinned to 0.10.0 (chill-java, chill_2.12) for Java 17 compatibility.
- Avoid introducing new logging backends or JSON libs without discussion.

## Testing
- Unit tests: `mvn test -DskipITs`
- Integration tests: `mvn test` (MiniCluster requires open ports; BLOB port set to `0` for ephemeral assignment).
- Mockito: subclass mock maker; JVM flag `--add-opens java.base/java.util=ALL-UNNAMED` in surefire.

## Dependencies
- See `docs/DESIGN-flink-job.md` for current pins.
- Prefer updating through the POM with clear rationale; note any overrides in docs.

## Safety / ops
- Do not remove MiniCluster port configuration (`BlobServerOptions.PORT=0`) without ensuring tests still start.
- Side outputs in `InputBroadcastProcessFunction` must stay aligned with block types.
- If adjusting serializers, consider Flink’s shaded vs. unshaded classes and Kryo registrations.

## Docs to keep in sync
- `docs/DESIGN-flink-job.md` for module design.
- `docs/ARCHITECTURE.md` for repo-level overview (stub today).
- This `AGENTS.md` for norms.

## Commit guidance
- Prefer small, focused commits (e.g., dependency bumps isolated from code refactors).
- Include rationale for version pins/overrides in commit messages when relevant.
