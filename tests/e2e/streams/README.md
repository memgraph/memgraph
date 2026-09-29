# Streams e2e tests

Kafka and Pulsar stream tests. The brokers run as two Docker Compose stacks in this directory:

* [kafka/docker-compose.yml](kafka/docker-compose.yml): single-node Kafka (KRaft, no ZooKeeper)
* [pulsar/docker-compose.yml](pulsar/docker-compose.yml): Pulsar standalone

Both stacks join the external `package_default` network, which is the network Compose creates for the
mgbuild container (see `release/package/mgbuild.sh`). That is how the tests and Memgraph reach the brokers
by service name when they run inside the build container, the way CI does.

## Broker addresses

| | Inside `package_default` (CI) | From the host (local default) |
|---|---|---|
| Kafka bootstrap | `kafka:9092` | `localhost:29092` |
| Pulsar service URL | `pulsar://pulsar:6650` | `pulsar://localhost:6650` |
| Pulsar admin URL | `http://pulsar:8080` | `http://localhost:6652` |

The tests read `KAFKA_BOOTSTRAP_SERVERS`, `PULSAR_SERVICE_URL` and `PULSAR_ADMIN_URL`, defaulting to the
host column. `workloads.yaml` passes the same values to Memgraph through `${VAR:-default}` expansion, and
`mgbuild.sh` exports the CI column when it runs the e2e suite in the container.

Kafka advertises `kafka:9092` on its internal listener and `localhost:29092` on the published one, so the
same broker serves both columns. Pulsar advertises a single address, `pulsar` by default; for clients on the
host start it with `PULSAR_ADVERTISED_ADDRESS=localhost`.

Both compose files carry a health check, so `docker compose up -d --wait` returns only once the broker
accepts requests.

## Running against the mgbuild container

With the build container running and Memgraph built (see `release/package/mgbuild.sh`; the container is
`mgbuild_<toolchain>_<os>`), start the stacks and run the streams workloads inside the container with the
CI addresses:

```
(cd kafka && docker compose up -d --wait)
(cd pulsar && docker compose up -d --wait)
docker exec -u mg mgbuild_v8_ubuntu-24.04 bash -c '
  source /opt/toolchain-v8/activate && source /home/mg/.cargo/env &&
  cd /home/mg/memgraph/tests && source ve3/bin/activate && cd e2e &&
  export KAFKA_BOOTSTRAP_SERVERS=kafka:9092 PULSAR_SERVICE_URL=pulsar://pulsar:6650 PULSAR_ADMIN_URL=http://pulsar:8080 &&
  python3 runner.py --workloads-root-directory streams'
```

Append `--workload-name "Kafka streams start, stop and show"` to run a single workload, or
`--workload-name-list` to list them. The SSO and stream-owner workloads also need
`MEMGRAPH_ENTERPRISE_LICENSE` and `MEMGRAPH_ORGANIZATION_NAME` exported in that shell. Memgraph writes its
logs under `/home/mg/memgraph/build/e2e/logs` in the container.

`mgbuild.sh ... test-memgraph e2e` runs the whole e2e suite, these workloads included, with the same
environment.

## Running on the host

```
docker network create package_default   # once, if no mgbuild container has ever run here
(cd kafka && docker compose up -d --wait)
(cd pulsar && PULSAR_ADVERTISED_ADDRESS=localhost docker compose up -d --wait)
cd .. && ./run.sh "Kafka streams start, stop and show"
```

Stop the stacks with `docker compose down` in each directory. Neither stack keeps a volume, so this also
discards the broker state, which is the fix if a broker misbehaves after an aborted run.
