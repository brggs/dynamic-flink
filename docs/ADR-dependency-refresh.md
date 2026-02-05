# ADR: Dependency Refresh Targets (Java module)

## Context
During the Flink 1.20 / Java 17 upgrade we identified several outdated or vulnerable dependencies in `flink-job/pom.xml`. We deferred the fixes to keep the main upgrade moving.

## Decision
Track and schedule a follow‑up task to modernize the dependencies listed below, prioritizing security and support for Java 17.

## Candidates to update/replace
- Replace Log4j 1.x stack (`slf4j-log4j12:1.7.7`, `log4j:1.2.17`) with Log4j 2 (`log4j-core/api/log4j-slf4j2-impl` 2.22.x).
- Align/remove direct Jackson deps (`jackson-databind 2.10.0.pr1`, `jackson-dataformat-cbor 2.6.7`); prefer ≥2.15.2 or rely on Flink’s shaded Jackson.
- Drop `commons-lang:2.6` (migrate to `commons-lang3` or JDK equivalents).
- Remove `commons-logging:1.1.3` (or bridge with `jcl-over-slf4j` if needed).
- Either remove or bump `joda-time:2.5` (prefer `java.time` or latest 2.12.x if still required).
- Consider replacing `jsoniter:0.9.19` with Jackson or updating to a maintained version.
- Chill is now explicitly pinned at `0.10.0` (`chill-java`, `chill_2.12`) to satisfy Flink Kryo; leave as-is unless Flink upgrades its BOM.

## Status
Pending. No code changes applied yet beyond noting the targets.

## Next Steps
1) Choose logging path (Log4j 2) and implement replacement.
2) Normalize Jackson versions or remove direct usages.
3) Clean up legacy utility libs (`commons-*`, `joda-time`, `jsoniter`) case-by-case in code.
