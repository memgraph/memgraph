# APOC to MAGE Translation Examples

This document provides concrete examples of translating Neo4j APOC procedures to Memgraph MAGE equivalents.

## Example 1: RDF Import (from Neosemantics)

### Original Neo4j Procedure

```
n10s.rdf.import.fetch(url, format, params)
-> (terminationStatus, triplesLoaded, triplesParsed, namespaces, extraInfo, callParams)
```

### Memgraph MAGE Translation

```python
"""
Module: rdf_util
Translated from: Neo4j Neosemantics n10s.rdf.import
"""
import mgp
import requests
from typing import Dict, Any, Optional


@mgp.write_proc
def fetch(
    ctx: mgp.ProcCtx,
    url: str,
    format: str,
    params: mgp.Nullable[mgp.Map] = None
) -> mgp.Record(
    terminationStatus=str,
    triplesLoaded=int,
    triplesParsed=int,
    namespaces=mgp.Map,
    extraInfo=str
):
    """
    Import RDF from URL and store as property graph.

    Parameters
    ----------
    url : str
        URL to fetch RDF data from (file:// or http://)
    format : str
        RDF format: Turtle, N-Triples, JSON-LD, RDF/XML
    params : Map, optional
        Configuration parameters

    Returns
    -------
    mgp.Record
        Import statistics and status
    """
    if params is None:
        params = {}

    triples_parsed = 0
    triples_loaded = 0
    namespaces = {}

    try:
        # Fetch RDF content
        if url.startswith("file://"):
            with open(url[7:], 'r') as f:
                content = f.read()
        else:
            response = requests.get(url, headers=params.get("headerParams", {}))
            content = response.text

        # Parse RDF based on format
        triples = parse_rdf(content, format)
        triples_parsed = len(triples)

        # Import into graph
        for subject, predicate, obj in triples:
            import_triple(ctx, subject, predicate, obj, params)
            triples_loaded += 1

        return mgp.Record(
            terminationStatus="OK",
            triplesLoaded=triples_loaded,
            triplesParsed=triples_parsed,
            namespaces=namespaces,
            extraInfo=""
        )
    except Exception as e:
        return mgp.Record(
            terminationStatus="KO",
            triplesLoaded=triples_loaded,
            triplesParsed=triples_parsed,
            namespaces=namespaces,
            extraInfo=str(e)
        )


def parse_rdf(content: str, format: str) -> list:
    """Parse RDF content into triples list."""
    # Implementation depends on format
    triples = []
    # ... parsing logic ...
    return triples


def import_triple(ctx: mgp.ProcCtx, subject, predicate, obj, params):
    """Import a single RDF triple into the graph."""
    # Find or create subject vertex
    # Create predicate as relationship or property
    # Handle object as vertex or literal
    pass
```

## Example 2: apoc.convert.toJson

### Original Neo4j

```cypher
RETURN apoc.convert.toJson({name: "John", age: 30}) AS json
-- Returns: '{"name":"John","age":30}'
```

### Memgraph MAGE Translation

```python
import mgp
import json


@mgp.function
def to_json(ctx: mgp.FuncCtx, value: mgp.Any) -> str:
    """
    Convert any value to JSON string.

    Handles vertices, edges, paths, maps, and primitives.
    """
    return json.dumps(_serialize(value))


def _serialize(value):
    """Recursively serialize Memgraph types to JSON-compatible types."""
    if isinstance(value, mgp.Vertex):
        return {
            "id": value.id,
            "labels": [l.name for l in value.labels],
            "properties": {p.name: _serialize(p.value) for p in value.properties.items()}
        }
    elif isinstance(value, mgp.Edge):
        return {
            "id": value.id,
            "type": value.type.name,
            "start": value.from_vertex.id,
            "end": value.to_vertex.id,
            "properties": {p.name: _serialize(p.value) for p in value.properties.items()}
        }
    elif isinstance(value, mgp.Path):
        return {
            "vertices": [_serialize(v) for v in value.vertices],
            "edges": [_serialize(e) for e in value.edges]
        }
    elif isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_serialize(v) for v in value]
    else:
        return value
```

## Example 3: apoc.refactor.mergeNodes

### Original Neo4j

```cypher
MATCH (p:Person)
WITH collect(p) AS persons
CALL apoc.refactor.mergeNodes(persons, {properties: "combine"})
YIELD node
RETURN node
```

### Memgraph MAGE Translation

```python
import mgp
from typing import List


@mgp.write_proc
def merge_nodes(
    ctx: mgp.ProcCtx,
    nodes: mgp.List[mgp.Vertex],
    config: mgp.Nullable[mgp.Map] = None
) -> mgp.Record(node=mgp.Vertex):
    """
    Merge multiple nodes into one.

    Parameters
    ----------
    nodes : List[Vertex]
        Nodes to merge (first node is kept)
    config : Map, optional
        Configuration:
        - properties: "discard" | "combine" | "overwrite"
        - mergeRels: bool (default: True)

    Returns
    -------
    mgp.Record
        The merged node
    """
    if not nodes:
        raise ValueError("At least one node required")

    if config is None:
        config = {}

    prop_strategy = config.get("properties", "combine")
    merge_rels = config.get("mergeRels", True)

    # Keep the first node
    target = nodes[0]

    for source in nodes[1:]:
        # Merge properties
        for prop in source.properties.items():
            if prop_strategy == "combine":
                existing = target.properties.get(prop.name)
                if existing is not None:
                    if isinstance(existing, list):
                        target.properties[prop.name] = existing + [prop.value]
                    else:
                        target.properties[prop.name] = [existing, prop.value]
                else:
                    target.properties[prop.name] = prop.value
            elif prop_strategy == "overwrite":
                target.properties[prop.name] = prop.value
            # "discard" does nothing

        # Merge labels
        for label in source.labels:
            try:
                target.add_label(label.name)
            except:
                pass

        # Redirect relationships
        if merge_rels:
            for edge in list(source.in_edges):
                if edge.from_vertex.id != target.id:
                    new_edge = ctx.graph.create_edge(
                        edge.from_vertex, target, edge.type
                    )
                    for p in edge.properties.items():
                        new_edge.properties[p.name] = p.value

            for edge in list(source.out_edges):
                if edge.to_vertex.id != target.id:
                    new_edge = ctx.graph.create_edge(
                        target, edge.to_vertex, edge.type
                    )
                    for p in edge.properties.items():
                        new_edge.properties[p.name] = p.value

        # Delete source node
        ctx.graph.detach_delete_vertex(source)

    return mgp.Record(node=target)
```

## Example 4: apoc.path.expand

### Original Neo4j

```cypher
MATCH (start:Person {name: "Alice"})
CALL apoc.path.expand(start, "KNOWS|FOLLOWS", "+Person", 1, 3)
YIELD path
RETURN path
```

### Memgraph MAGE Translation

```python
import mgp
from typing import Set


@mgp.read_proc
def expand(
    ctx: mgp.ProcCtx,
    start: mgp.Vertex,
    rel_filter: str,
    label_filter: str,
    min_depth: int = 1,
    max_depth: int = -1
) -> mgp.Record(path=mgp.Path):
    """
    Expand paths from start node with filters.

    Parameters
    ----------
    start : Vertex
        Starting node
    rel_filter : str
        Relationship types to traverse (e.g., "KNOWS|FOLLOWS", ">KNOWS" for outgoing)
    label_filter : str
        Label filter ("+Label" include, "-Label" exclude)
    min_depth : int
        Minimum path length
    max_depth : int
        Maximum path length (-1 for unlimited)
    """
    # Parse relationship filter
    rel_types, direction = _parse_rel_filter(rel_filter)

    # Parse label filter
    include_labels, exclude_labels = _parse_label_filter(label_filter)

    results = []

    def dfs(current_path: mgp.Path, visited: Set[int], depth: int):
        if max_depth != -1 and depth > max_depth:
            return

        if depth >= min_depth:
            results.append(mgp.Record(path=current_path))

        current_vertex = current_path.vertices[-1]

        edges = []
        if direction in ("both", "out"):
            edges.extend(current_vertex.out_edges)
        if direction in ("both", "in"):
            edges.extend(current_vertex.in_edges)

        for edge in edges:
            # Check relationship type filter
            if rel_types and edge.type.name not in rel_types:
                continue

            # Get next vertex
            if edge.from_vertex.id == current_vertex.id:
                next_vertex = edge.to_vertex
            else:
                next_vertex = edge.from_vertex

            # Check if already visited
            if next_vertex.id in visited:
                continue

            # Check label filter
            next_labels = {l.name for l in next_vertex.labels}
            if include_labels and not (next_labels & include_labels):
                continue
            if exclude_labels and (next_labels & exclude_labels):
                continue

            # Extend path
            import copy
            new_path = copy.copy(current_path)
            new_path.expand(edge)

            new_visited = visited | {next_vertex.id}
            dfs(new_path, new_visited, depth + 1)

    initial_path = mgp.Path(start)
    dfs(initial_path, {start.id}, 0)

    return results


def _parse_rel_filter(filter_str: str):
    """Parse relationship filter string."""
    direction = "both"
    if filter_str.startswith(">"):
        direction = "out"
        filter_str = filter_str[1:]
    elif filter_str.startswith("<"):
        direction = "in"
        filter_str = filter_str[1:]

    rel_types = set(filter_str.split("|")) if filter_str else set()
    return rel_types, direction


def _parse_label_filter(filter_str: str):
    """Parse label filter string."""
    include = set()
    exclude = set()

    for part in filter_str.split("|"):
        part = part.strip()
        if part.startswith("+"):
            include.add(part[1:])
        elif part.startswith("-"):
            exclude.add(part[1:])
        elif part:
            include.add(part)

    return include, exclude
```

## Example 5: apoc.periodic.iterate

### Original Neo4j

```cypher
CALL apoc.periodic.iterate(
  "MATCH (n:Person) RETURN n",
  "SET n.processed = true",
  {batchSize: 1000}
)
```

### Memgraph MAGE Translation

```python
import mgp


@mgp.write_proc
def iterate(
    ctx: mgp.ProcCtx,
    cypher_iterate: str,
    cypher_action: str,
    config: mgp.Nullable[mgp.Map] = None
) -> mgp.Record(
    batches=int,
    total=int,
    timeTaken=int,
    committedOperations=int,
    failedOperations=int,
    failedBatches=int,
    errorMessages=mgp.Map
):
    """
    Iterate over query results and apply action in batches.

    Note: In Memgraph, this is typically handled differently.
    Consider using the built-in batch execution features.

    This implementation provides compatibility with APOC patterns.
    """
    import time
    import gqlalchemy

    if config is None:
        config = {}

    batch_size = config.get("batchSize", 10000)

    start_time = time.time()
    total = 0
    committed = 0
    failed = 0
    failed_batches = 0
    errors = {}
    batches = 0

    memgraph = gqlalchemy.Memgraph()

    try:
        # Execute iteration query
        results = list(memgraph.execute_and_fetch(cypher_iterate))
        total = len(results)

        # Process in batches
        for i in range(0, total, batch_size):
            batch = results[i:i + batch_size]
            batches += 1

            try:
                for row in batch:
                    # Execute action for each row
                    # Substitute row values into action query
                    action_query = _substitute_params(cypher_action, row)
                    memgraph.execute(action_query)
                    committed += 1
            except Exception as e:
                failed += len(batch) - (committed % batch_size)
                failed_batches += 1
                errors[str(batches)] = str(e)

    except Exception as e:
        errors["iteration"] = str(e)

    time_taken = int((time.time() - start_time) * 1000)

    return mgp.Record(
        batches=batches,
        total=total,
        timeTaken=time_taken,
        committedOperations=committed,
        failedOperations=failed,
        failedBatches=failed_batches,
        errorMessages=errors
    )


def _substitute_params(query: str, row: dict) -> str:
    """Substitute row values into query."""
    # Simple substitution - production code should use parameterized queries
    for key, value in row.items():
        query = query.replace(f"${key}", _to_cypher_literal(value))
    return query


def _to_cypher_literal(value) -> str:
    """Convert Python value to Cypher literal."""
    if isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif value is None:
        return "null"
    else:
        return str(value)
```

## Mapping Quick Reference

| APOC Procedure | MAGE Module | Notes |
|----------------|-------------|-------|
| `apoc.create.node` | `create.node` | Direct translation |
| `apoc.create.relationship` | `create.relationship` | Direct translation |
| `apoc.refactor.mergeNodes` | `refactor.merge_nodes` | camelCase → snake_case |
| `apoc.convert.toJson` | `json_util.to_json` | Module name differs |
| `apoc.path.expand` | `path.expand` | Implement with DFS/BFS |
| `apoc.periodic.iterate` | `periodic.iterate` | Consider native batching |
| `n10s.rdf.import.fetch` | `rdf_util.fetch` | Requires RDF parser |
| `apoc.meta.schema` | `meta_util.schema` | Query schema info |

## Adding to mappings.json

After implementing each procedure, add its mapping to `config/mappings.json`:

```json
{
    "apoc.create.node": "create.node",
    "apoc.create.relationship": "create.relationship",
    "apoc.refactor.mergeNodes": "refactor.merge_nodes",
    "apoc.convert.toJson": "json_util.to_json",
    "apoc.path.expand": "path.expand",
    "apoc.periodic.iterate": "periodic.iterate",
    "n10s.rdf.import.fetch": "rdf_util.fetch",
    "apoc.meta.schema": "meta_util.schema"
}
```

This enables Neo4j-compatible syntax like:
```cypher
-- Both work after adding mapping:
CALL apoc.refactor.mergeNodes(nodes) YIELD node;  -- Neo4j syntax
CALL refactor.merge_nodes(nodes) YIELD node;       -- Memgraph native
```

## Creating Tests from Neo4j Documentation

When translating an APOC procedure, extract examples from the Neo4j documentation to create test cases.

### Example: n10s.rdf.import (from Neosemantics docs)

**Neo4j documentation shows:**

```cypher
CALL n10s.rdf.import.fetch(
  "https://github.com/neo4j-labs/neosemantics/raw/3.5/docs/rdf/nsmntx.ttl",
  "Turtle"
);
-- Returns: terminationStatus: "OK", triplesLoaded: 19, triplesParsed: 19
```

**Create test directory:**

```
mage/tests/e2e/rdf_util_test/
├── test_fetch_turtle_url/
│   ├── input.cyp
│   └── test.yml
├── test_fetch_invalid_format/
│   ├── input.cyp
│   └── test.yml
└── test_fetch_file_not_found/
    ├── input.cyp
    └── test.yml
```

**test_fetch_turtle_url/input.cyp** (empty - no setup needed)

**test_fetch_turtle_url/test.yml:**

```yaml
query: >
  CALL rdf_util.fetch(
    "https://example.com/test.ttl",
    "Turtle"
  ) YIELD terminationStatus, triplesLoaded
  RETURN terminationStatus, triplesLoaded;

output:
  - terminationStatus: "OK"
    triplesLoaded: 19
```

**test_fetch_invalid_format/test.yml:**

```yaml
query: >
  CALL rdf_util.fetch("https://example.com/test.ttl", "InvalidFormat")
  YIELD terminationStatus
  RETURN terminationStatus;

exception: >-
  "Unsupported RDF format: InvalidFormat"
```

### Example: apoc.map.flatten (from APOC docs)

**Neo4j documentation shows:**

```cypher
RETURN apoc.map.flatten({
  person: {name: "Cristiano Ronaldo", club: {name: "Al-Nassr", location: "Arabia"}},
  a: "b"
}) AS result
-- Returns: {a: "b", person.name: "Cristiano Ronaldo", person.club.name: "Al-Nassr", person.club.location: "Arabia"}
```

**Create test:**

```
mage/tests/e2e/map_test/test_flatten_nested_deep/
├── input.cyp    (empty)
└── test.yml
```

**test.yml:**

```yaml
query: >
  RETURN map.flatten({
    person: {name: "Cristiano Ronaldo", club: {name: "Al-Nassr", location: "Arabia"}},
    a: "b"
  }) AS result;

output:
  - result: {"a": "b", "person.club.location": "Arabia", "person.club.name": "Al-Nassr", "person.name": "Cristiano Ronaldo"}
```

### Example: apoc.refactor.mergeNodes

**Neo4j documentation shows:**

```cypher
MATCH (p:Person)
WITH collect(p) AS persons
CALL apoc.refactor.mergeNodes(persons, {properties: "combine"})
YIELD node
RETURN node
```

**Create tests:**

```
mage/tests/e2e/refactor_test/
├── test_merge_nodes_combine_basic/
│   ├── input.cyp
│   └── test.yml
├── test_merge_nodes_discard/
│   ├── input.cyp
│   └── test.yml
├── test_merge_nodes_overwrite/
│   ├── input.cyp
│   └── test.yml
└── test_merge_nodes_empty_list/
    ├── input.cyp
    └── test.yml
```

**test_merge_nodes_combine_basic/input.cyp:**

```cypher
CREATE (n1:Person {name: 'Alice', age: 30, city: 'New York'});
CREATE (n2:Person {name: 'Bob', age: 25, country: 'USA'});
```

**test_merge_nodes_combine_basic/test.yml:**

```yaml
query: >
  MATCH (n1:Person {name: 'Alice'}), (n2:Person {name: 'Bob'})
  CALL refactor.merge_nodes([n1, n2], {properties: 'combine'}) YIELD node
  RETURN node.name as name, node.age as age, node.city as city, node.country as country;

output:
  - name: [Alice, Bob]
    age: [30, 25]
    city: New York
    country: USA
```

**test_merge_nodes_empty_list/test.yml:**

```yaml
query: >
  CALL refactor.merge_nodes([], {properties: 'combine'}) YIELD node
  RETURN node;

exception: >-
  "The nodes list cannot be empty"
```

### Test Coverage Checklist

For each procedure, ensure you have tests for:

| Test Type | Example | Purpose |
|-----------|---------|---------|
| Basic usage | `test_flatten_basic` | Verify happy path |
| Complex input | `test_flatten_nested_deep` | Test with realistic data |
| Empty input | `test_flatten_empty` | Handle edge case |
| Null input | `test_flatten_null` | Handle null gracefully |
| Invalid input | `test_flatten_invalid_type` | Verify error handling |
| Doc example | `test_flatten_doc_example` | Match Neo4j behavior |
