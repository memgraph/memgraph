# Stage 1: Recording Memgraph Execution

## Objective
Record memgraph execution under live-recorder with test Cypher queries.

## Prerequisites
- RelWithDebInfo build of memgraph at `./build/memgraph`
- Undo live-recorder available

## Memgraph Flags Used
```
--cartesian-product-enabled false
--data-recovery-on-startup false
--debug-query-plans true
--log-level DEBUG
--schema-info-enabled true
--storage-enable-edges-metadata true
--storage-enable-schema-metadata true
--storage-gc-aggressive false
--storage-properties-on-edges true
--storage-snapshot-on-exit false
--storage-wal-enabled false
--telemetry-enabled false
--query-modules-directory ./build/query_modules
```

## Test Queries
```cypher
CREATE (a:Person {name: 'Alice', age: 30});
CREATE (b:Person {name: 'Bob', age: 25});
CREATE (c:Person {name: 'Charlie', age: 35});
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b);
MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c);
MATCH (p:Person) RETURN p.name, p.age;
MATCH (a)-[r:KNOWS]->(b) RETURN a.name, b.name;
```

## Execution
Run `script_1.sh` to perform the recording.

## Output
Recording saved to: `findings_1/memgraph_session.undo`
