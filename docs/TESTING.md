# Testing Guide (repo-level)

## Commands
- Unit tests only: `mvn test -DskipITs`
- Full suite (includes Flink MiniCluster integration tests): `mvn test`

## Flink MiniCluster notes
- MiniCluster needs to bind a BLOB server port. For tests, it defaults to an ephemeral port via system property `flink.blob.port` (default `0`). Override as needed:
  - `mvn test -Dflink.blob.port=61234`
- Ensure the environment allows opening local ports; sandboxed environments may fail integration tests.

## JPMS / Mockito
- JVM arg set in surefire: `--add-opens java.base/java.util=ALL-UNNAMED`
- Mockito uses subclass mock maker; inline agent attach is disabled.

## Logging
- Test logging configured by `src/test/resources/log4j2-test.xml` (Flink/Kafka/Testcontainers at WARN, root INFO).

## Troubleshooting
- Port binding errors: set `-Dflink.blob.port` to an open port.
- Multiple SLF4J bindings: ensure only Log4j2 binding is on the classpath; Log4j 1.x removed.
