## Local test to check whether run_id is inside query metadata

In order to run this test, you should enable telemetry while running Memgraph,
because telemetry generated the run_id of a Memgraph instance.

Before doing that, we need to restrict the communication with memgraph's
telemetry server, so tests wouldn't give us garbage telemetry. To do that you
should add:
```
127.0.0.1   telemetry.memgraph.com
```
to the beginning of `/etc/hosts`.

Now that you blocked telemetry from being sent, you should run memgraph with
telemetry enabled:
```
memgraph --telemetry-enabled=True
```
Finally, to run the test:
```
pnpm install --frozen-lockfile
node get-run_id.js
```

Output should be:
```
Query: MATCH (n) RETURN n LIMIT 1;
run_id: <your-memgraph-run_id>
```
