# Jepsen Memgraph Test Suite

NOTE: Jepsen can only connect to the SSH server on the default 22 port.
`--node` flag only takes the actual address (:port doesn't work).

Jepsen run under CI:
```
cd tests/jepsen
./run.sh test --binary ../../build/memgraph --run-args "test-all --node-configs resources/node-config.edn" --ignore-run-stdout-logs --ignore-run-stderr-logs
```

Local run of each test (including setup):
```
cd tests/jepsen
./run.sh cluster-up
docker exec -it jepsen-control bash
cd memgraph
lein run test --workload bank --node-configs resources/node-config.edn
lein run test --workload large --node-configs resources/node-config.edn
```

Logs are located under `jepsen-control:/jepsen/memgraph/store`.
