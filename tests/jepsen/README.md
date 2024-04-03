# Jepsen Memgraph Test Suite

NOTE: Jepsen can only connect to the SSH server on the default 22 port.
`--node` flag only takes the actual address (:port doesn't work).

Jepsen run under CI:
```
cd tests/jepsen
./run.sh test --binary ../../build/memgraph --run-args "test-all --node-configs resources/node-config.edn" --ignore-run-stdout-logs --ignore-run-stderr-logs
./run.sh test --binary ../../build/memgraph --run-args "test --workload bank --node-configs resources/node-config.edn" --ignore-run-stdout-logs --ignore-run-stderr-logs
```

Local run of each test (including setup):
```
cd tests/jepsen
./run.sh cluster-up --binary ../../build/memgraph
docker exec -it jepsen-control bash
cd memgraph
lein run test --workload bank --node-configs resources/node-config.edn
lein run test --workload large --node-configs resources/node-config.edn
lein run test --workload high_availability --node-configs resources/cluster.edn
```

Logs are located under `jepsen-control:/jepsen/memgraph/store`.

If you setup cluster manually go to jepsen-control Docker container and ssh to all cluster nodes to save their host keys in known_hosts.
```
docker exec -it jepsen-control bash
ssh n1 -> yes -> exit
ssh n2 -> yes -> exit
ssh n3 -> yes -> exit
ssh n4 -> yes -> exit
ssh n5 -> yes -> exit
```
