# Native Memgraph Monitoring (Docker Compose)

This bundle reuses the same remote monitoring flow as `victoria-cluster`, but for Memgraph processes running inside mgbuild containers.

- Metrics: `Memgraph -> mg-exporter -> vmagent -> VictoriaMetrics`
- Logs: `Memgraph log files -> vector -> VictoriaLogs`

## What this starts

- `mg-exporter` (`memgraph/prometheus-exporter:0.2.1`)
- `vmagent` (`victoriametrics/vmagent:latest`)
- `vector` (`timberio/vector:0.42.0-debian`)

## Prerequisites

- Docker with Compose support (`docker compose`)
- `envsubst`
- mgbuild services started from `release/package/*-builders-v7.yml`
- `mgbuild_network` Docker network exists
- shared Docker volume exists (default `memgraph_logs`)
- Memgraph instances write logs under the shared mount path, default `/home/mg/memgraph-logs`

## Endpoint configuration

Provide either full URLs:

- `REMOTE_WRITE_URL` (example: `http://192.168.0.25:30091/insert/0/prometheus/api/v1/write`)
- `VLOGS_PUSH_URL` (example: `http://192.168.0.25:30454/insert/loki/api/v1/push`)

or host + optional ports/schemes:

- `MONITORING_SERVER_HOST` (example: `192.168.0.25`)
- `REMOTE_WRITE_PORT` (default: `30091`)
- `VLOGS_PORT` (default: `30454`)
- `REMOTE_WRITE_SCHEME` / `VLOGS_SCHEME` (default: `http`)

## Optional variables

- `CLUSTER_ID` (default: empty)
- `CLUSTER_ENV` (default: `ci-standalone-victoria`)
- `MEMGRAPH_METRICS_HOST` (default: `host.docker.internal`)
- `MEMGRAPH_METRICS_PORT` (default: `9091`)
- `MEMGRAPH_METRICS_TARGETS` (comma-separated, e.g. `mgbuild_v7_ubuntu-24.04:9091,mgbuild_v7_debian-12:9091`; enables multi-instance exporter mode)
- `MEMGRAPH_LOGS_VOLUME` (default: `memgraph_logs`)
- `MEMGRAPH_LOGS_PATH_IN_VOLUME` (default: `/home/mg/memgraph-logs`)
- `MEMGRAPH_LOG_GLOB` (default: `/home/mg/memgraph-logs/**/memgraph*.log`)
- `COMPOSE_PROJECT_NAME` (default: `memgraph-monitoring`)

## Multi-instance notes

- One `mg-exporter` can scrape multiple Memgraph instances using `MEMGRAPH_METRICS_TARGETS`.
- One `vmagent` is enough because it scrapes only `mg-exporter`.
- One `vector` is enough when logs are in the shared volume and matched by `MEMGRAPH_LOG_GLOB`.
- For parallel monitoring stacks, use different `COMPOSE_PROJECT_NAME` values.

## Start

```bash
cd tools/ci/monitoring

MONITORING_SERVER_HOST=192.168.0.25 \
CLUSTER_ID=local-dev \
CLUSTER_ENV=ci-native \
MEMGRAPH_METRICS_TARGETS=mgbuild_v7_ubuntu-24.04:9091,mgbuild_v7_debian-12:9091 \
./up.sh
```

## Stop

```bash
cd tools/ci/monitoring
./down.sh
```

## Generated files

`up.sh` renders runtime files in `generated/`:

- `generated/mg-exporter.yaml`
- `generated/vmagent-scrape.yml`
- `generated/vector.yaml`
