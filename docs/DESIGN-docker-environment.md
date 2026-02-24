# Docker Compose Environment Design

## Purpose

Define a new containerized development and deployment environment using Docker Compose.  
This environment replaces the legacy setup currently in `demo-environment/`.

## Goals

- Provide a single, reproducible local stack for development and integration testing.
- Support deployment-style runs with minimal changes from local configuration.
- Remove ZooKeeper dependency by using Kafka in KRaft mode.
- Keep service contracts explicit (ports, topics, indices, schemas, DB state).
- Make it easy to evolve via Compose profiles and environment overrides.

## Components

| Service | Purpose | Initial Compose Scope |
|---------|---------|-----------------------|
| **Flink JobManager** | Cluster coordination | Included |
| **Flink TaskManager** | Job execution | Included |
| **Kafka (KRaft)** | Message streaming (no Zookeeper required) | Included |
| **Kafka Connect** | Sink connector for Kafka -> Elasticsearch | Included |
| **PostgreSQL** | Rule metadata, user accounts, app config | Deferred |
| **Elasticsearch** | Match result storage and search | Included |
| **Frontend** | Web UI container | Deferred |
| **Event Generator** | Test data generation | Included |

## Initial Implementation Scope (Phase 1)

The first `compose.yaml` should include:

- Flink JobManager
- Flink TaskManager
- Kafka (KRaft)
- Kafka Connect
- Elasticsearch
- Event Generator

Deferred for now (explicitly not in initial Compose):

- Frontend
- PostgreSQL

## Folder Layout

Design doc location:

```text
docs/
  DESIGN-docker-environment.md
```

Planned runtime assets (separate from `demo-environment/`):

```text
docker-environment/
  compose.yaml                 # planned
  .env.example                 # planned
  connectors/                  # planned (Kafka Connect plugin/config assets)
  init/
    postgres/                  # planned (schema/bootstrap SQL)
    elasticsearch/             # planned (index template/bootstrap)
```

## High-Level Topology

- `event-generator` produces events to Kafka input topic(s).
- Flink consumes input events and control/rule streams, emits match/output events.
- Kafka Connect consumes Kafka output topic(s) and writes to Elasticsearch indices.
- Frontend integration is deferred until a stable UI/API contract is ready.
- PostgreSQL-backed metadata is deferred until persistence requirements are finalized.

## Compose Design

### Service Boundaries

- **Flink**
  - `jobmanager` and one or more `taskmanager` services.
  - Job artifact mounted or baked into image depending on profile.
- **Kafka**
  - Single broker KRaft for local dev.
  - Listener split for in-network (`kafka:9092`) and host access (`localhost:29092`).
- **Kafka Connect**
  - Dedicated worker image with required sink connector plugin(s).
  - Connector config managed as declarative JSON assets and applied at startup.
- **PostgreSQL**
  - Deferred from initial Compose.
  - Will be added with persistent volume and initialization scripts in a later phase.
- **Elasticsearch**
  - Single node for local/dev profile.
  - Persistent volume and startup healthcheck.
- **Frontend**
  - Deferred from initial Compose.
  - Will be added after backend/API contracts are in place.
- **Event Generator**
  - Lightweight producer used for smoke/integration workflows.

### Networking

- Single user-defined bridge network for all services.
- Internal DNS names used in all inter-service configs.
- Only required ports exposed to host:
  - Flink UI
  - Kafka external listener
  - Elasticsearch (if direct access is needed)

### Persistence

- Named volumes:
  - `kafka-data`
  - `elasticsearch-data`
- Non-persistent services by default:
  - Flink TaskManagers
  - Event generator

### Health and Startup

- Add healthchecks for Kafka, Elasticsearch, and Kafka Connect.
- Use `depends_on` with health conditions to improve deterministic startup.
- Include a `make`/script entrypoint for:
  - Boot stack
  - Apply connector config
  - Run smoke checks

### Configuration Strategy

- `compose.yaml` stores defaults for local development.
- `.env` (from `.env.example`) controls mutable settings:
  - image tags
  - topic names
  - credentials
  - ports
  - resource sizing hints
- Optional `compose.override.yaml` for developer-local customizations.
- Optional `compose.prod.yaml` for deployment-oriented overrides.

## Planned Profiles

- `dev`:
  - source-mounted event generator when useful
  - verbose logging
  - local-friendly resource settings
- `deploy`:
  - immutable images
  - tighter restart policies
  - conservative log verbosity

## Migration From Legacy Demo Environment

Current legacy assets in `demo-environment/` are based on older Flink/Kafka patterns and ZooKeeper.  
Migration approach:

1. Introduce new `docker-environment/compose.yaml` and supporting assets.
2. Validate parity of core flows:
   - event in -> Flink -> Kafka output -> Elasticsearch sink.
3. Validate operational endpoints:
   - Flink UI, Elasticsearch availability, connector health.
4. Update root documentation to point to the new folder.
5. Mark `demo-environment/` as deprecated, then remove after cutoff.

## Non-Goals (Initial Cut)

- Multi-broker Kafka production topology.
- Elasticsearch clustering beyond single-node local/dev needs.
- Kubernetes manifests (can follow after Compose stabilization).
- Full secrets management system (beyond env-file strategy).

## Open Decisions

- Which Kafka Connect distribution/image to standardize on.
- Whether frontend should talk directly to Elasticsearch or via API layer.
- Exact strategy for submitting Flink jobs (container command vs REST submission job).
- Minimum supported Docker resources for stable local runs.

## Acceptance Criteria For Next Step

- A first runnable `compose.yaml` exists under `docker-environment/`.
- Initial `compose.yaml` excludes deferred services (`frontend`, `postgres`).
- `docker compose up` starts included services cleanly with healthy status.
- A smoke test demonstrates end-to-end data flow into Elasticsearch.
- New environment instructions replace demo-environment startup instructions.
