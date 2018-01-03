# Raft Tests

To run test locally execute following command:

```
./local_runner {test_suite} {test_name}
```

Every test has to be defined as python module
with exposed ```run(machine_ids, workers)```
method. In each test there has to be constant
```NUM_MACHINES``` which specifies how many workers
to run in cluster.
