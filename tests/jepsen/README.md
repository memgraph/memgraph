# Jepsen Memgraph Test Suite

NOTE: Jepsen can only connect to the SSH server on the default 22 port.
`--node` flag only takes the actual address (:port doesn't work).

Resources folder contains files in which config for each instance is specified. replication-config.edn is used for replication tests (bank, large).

Jepsen run under CI:
```
cd tests/jepsen
./run.sh test-all-individually --binary ../../build/memgraph --run-args "--time-limit 120"
./run.sh test --binary ../../build/memgraph --run-args "--workload bank --nodes-config resources/replication-config.edn --time-limit 120"
./run.sh test --binary ../../build/memgraph --run-args "--workload large --nodes-config resources/replication-config.edn --time-limit 120"
./run.sh test --binary ../../build/memgraph --run-args "--workload high_availability --nodes-config resources/cluster.edn --time-limit 120"
```

Local run of each test (including setup):

You have to compile Memgraph for debian-12 in order to locally use Jepsen.

```
cd tests/jepsen
./run.sh cluster-up --binary ../../build/memgraph
docker exec -it jepsen-control bash
cd memgraph
lein run test-all --nodes-config resources/replication-config.edn --time-limit 120
lein run test --workload bank --nodes-config resources/replication-config.edn --time-limit 120
lein run test --workload large --nodes-config resources/replication-config.edn --time-limit 120
lein run test --workload high_availability --nodes-config resources/cluster.edn
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

There is also a unit test in test/jepsen/memgraph/memgraph_test.clj with sanitizers tests for Clojure code being run before main Jepsen tests.
You can run it with:

```
/run.sh unit-tests --binary ../../build/memgraph
```
