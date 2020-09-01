# Dynamic Flink Demo Environment

To see Dynamic Flink in action, this Docker Compose environment provides all of the necessary services, as well as a UI showing what the system is doing, and allowing rules to be added and configured.

## Prerequisites

Docker is the only prerequisite, but note that currently it is only possible to build the Flink docker image on MacOS/Linux.

## Steps to build the flink job docker image

1) Build the Flink job (`cd flink-job`, `mvn package`)
2) Download Flink (flink-1.10.1-bin-scala_2.11.tgz) to `demo-environment/flink-docker/`. Find the link from https://www.apache.org/dyn/closer.lua/flink/flink-1.10.1/flink-1.10.1-bin-scala_2.11.tgz
3) `cd demo-environment/flink-docker`
4) `./build.sh --job-artifacts ../../flink-job/target/dynamic-flink-0.1.0-SNAPSHOT.jar --from-archive flink-1.10.1-bin-scala_2.11.tgz --image-name dynamic-flink-job`

## Starting the Demo Environment

1) `cd demo-environment`
2) `docker-compose up`

Once everything has started, the UI will be available at:
http://localhost:3001/

If you make changes to any of the services, use `docker-compose up --build` to rebuild before starting.
