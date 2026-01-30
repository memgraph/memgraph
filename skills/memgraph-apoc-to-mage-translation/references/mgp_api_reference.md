# Memgraph Python API (mgp) Reference

This reference covers the key classes, decorators, and types available in the Memgraph Python API for writing query modules.

## Decorators

### @mgp.read_proc

Register a read-only procedure (cannot modify the graph).

```python
@mgp.read_proc
def my_procedure(
    ctx: mgp.ProcCtx,
    param1: str,
    param2: mgp.Nullable[int] = None
) -> mgp.Record(result=str):
    return mgp.Record(result="value")
```

### @mgp.write_proc

Register a procedure that can modify the graph.

```python
@mgp.write_proc
def my_write_procedure(
    ctx: mgp.ProcCtx,
    label: str
) -> mgp.Record(node=mgp.Vertex):
    vertex = ctx.graph.create_vertex()
    vertex.add_label(label)
    return mgp.Record(node=vertex)
```

### @mgp.function

Register a user-defined function (callable in Cypher expressions).

```python
@mgp.function
def my_function(
    ctx: mgp.FuncCtx,
    input_str: str
) -> str:
    return input_str.upper()
```

## Context Classes

### mgp.ProcCtx

Context for procedure execution. Provides access to the graph.

```python
ctx.graph          # Access to Graph object
ctx.must_abort()   # Check if procedure should abort
ctx.check_must_abort()  # Raises AbortError if should abort
```

### mgp.FuncCtx

Context for function execution. Read-only graph access.

```python
ctx._graph  # Read-only Graph access (not mutable)
```

## Graph Classes

### mgp.Graph

Represents the graph database state.

```python
# Properties
graph.vertices     # Vertices collection (iterable)

# Methods
graph.get_vertex_by_id(vertex_id: int) -> Vertex
graph.is_mutable() -> bool
graph.create_vertex() -> Vertex           # Only in write procedures
graph.delete_vertex(vertex: Vertex)       # Only in write procedures
graph.detach_delete_vertex(vertex: Vertex)  # Delete vertex and all edges
graph.create_edge(from_v: Vertex, to_v: Vertex, edge_type: EdgeType) -> Edge
graph.delete_edge(edge: Edge)
```

### mgp.Vertex

Represents a node in the graph.

```python
# Properties
vertex.id          # VertexId (int)
vertex.labels      # Tuple[Label, ...]
vertex.properties  # Properties collection
vertex.in_edges    # Iterable[Edge] - incoming edges
vertex.out_edges   # Iterable[Edge] - outgoing edges

# Methods
vertex.add_label(label: str)     # Only in write procedures
vertex.remove_label(label: str)  # Only in write procedures
vertex.is_valid() -> bool
```

### mgp.Edge

Represents a relationship in the graph.

```python
# Properties
edge.id            # EdgeId (int)
edge.type          # EdgeType
edge.from_vertex   # Source Vertex
edge.to_vertex     # Destination Vertex
edge.properties    # Properties collection

# Methods
edge.is_valid() -> bool
```

### mgp.Label

Represents a vertex label.

```python
label.name  # str - the label name
```

### mgp.EdgeType

Represents a relationship type.

```python
edge_type = mgp.EdgeType("KNOWS")
edge_type.name  # str - the type name
```

### mgp.Properties

Collection of properties on a Vertex or Edge.

```python
# Access
props.get(name: str, default=None) -> object
props[name]  # Raises KeyError if not found
name in props  # Check if property exists

# Modification (write procedures only)
props.set(name: str, value: object)
props[name] = value
props.set_properties(properties: dict)

# Iteration
props.items() -> Iterable[Property]  # (name, value) pairs
props.keys() -> Iterable[str]
props.values() -> Iterable[object]
len(props)
```

### mgp.Path

Represents a path in the graph.

```python
path = mgp.Path(starting_vertex)
path.expand(edge)  # Append edge to path
path.pop()         # Remove last edge
path.vertices      # Tuple[Vertex, ...]
path.edges         # Tuple[Edge, ...]
path.length        # Number of edges
```

## Type Annotations

### Basic Types

```python
str           # String
int           # Integer
float         # Float
bool          # Boolean
```

### Memgraph-Specific Types

```python
mgp.Vertex          # Node
mgp.Edge            # Relationship
mgp.Path            # Path
mgp.Map             # Map/Dictionary (Union[dict, Edge, Vertex])
mgp.Any             # Any Cypher value
mgp.Number          # Union[int, float]
```

### Container Types

```python
mgp.List[T]         # List of type T
mgp.Nullable[T]     # Optional type (can be None/null)
```

### Temporal Types

```python
mgp.Date            # datetime.date
mgp.LocalTime       # datetime.time
mgp.LocalDateTime   # datetime.datetime
mgp.Duration        # datetime.timedelta
```

## Return Types

### mgp.Record

Define procedure return fields.

```python
# Single field
-> mgp.Record(result=str)

# Multiple fields
-> mgp.Record(node=mgp.Vertex, count=int, name=str)

# Deprecated field
-> mgp.Record(old_field=mgp.Deprecated(str), new_field=str)

# Return single record
return mgp.Record(result="value")

# Return multiple records (list)
return [mgp.Record(n=v) for v in vertices]

# Return no results (empty procedure)
return mgp.Record()
```

## Exceptions

```python
mgp.InvalidContextError    # Using graph element outside procedure
mgp.UnknownError           # Unspecified failure
mgp.UnableToAllocateError  # Memory allocation failed
mgp.OutOfRangeError        # Index out of bounds
mgp.LogicErrorError        # Logic/precondition violation
mgp.DeletedObjectError     # Accessing deleted object
mgp.InvalidArgumentError   # Invalid argument value
mgp.KeyAlreadyExistsError  # Key exists in container
mgp.ImmutableObjectError   # Modifying immutable object
mgp.ValueConversionError   # Python/Cypher conversion failed
mgp.SerializationError     # Concurrent modification conflict
mgp.AuthorizationError     # Insufficient permissions
mgp.AbortError             # Procedure abort requested
```

## Logging

```python
logger = mgp.Logger()
logger.info("Information message")
logger.warning("Warning message")
logger.error("Error message")
logger.critical("Critical message")
logger.debug("Debug message")
logger.trace("Trace message")
```

## Stream Transformations

For Kafka/Pulsar stream processing:

```python
@mgp.transformation
def my_transform(
    ctx: mgp.TransCtx,  # Optional
    messages: mgp.Messages
) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    results = []
    for i in range(messages.total_messages()):
        msg = messages.message_at(i)
        payload = msg.payload().decode('utf-8')
        results.append(mgp.Record(
            query=f"CREATE (n:Event {{data: '{payload}'}})",
            parameters=None
        ))
    return results
```

## Complete Example Module

```python
"""
Example MAGE module demonstrating common patterns.
"""
import mgp
from typing import List, Dict, Any


@mgp.read_proc
def get_neighbors(
    ctx: mgp.ProcCtx,
    node: mgp.Vertex,
    max_depth: int = 1
) -> mgp.Record(neighbor=mgp.Vertex, depth=int):
    """
    Get all neighbors of a node up to max_depth.

    Parameters
    ----------
    node : mgp.Vertex
        Starting node
    max_depth : int
        Maximum traversal depth (default: 1)

    Returns
    -------
    mgp.Record
        neighbor: Neighboring vertex
        depth: Distance from start node
    """
    results = []
    visited = {node.id}
    current_level = [node]

    for depth in range(1, max_depth + 1):
        next_level = []
        for v in current_level:
            for edge in v.out_edges:
                neighbor = edge.to_vertex
                if neighbor.id not in visited:
                    visited.add(neighbor.id)
                    next_level.append(neighbor)
                    results.append(mgp.Record(neighbor=neighbor, depth=depth))
            for edge in v.in_edges:
                neighbor = edge.from_vertex
                if neighbor.id not in visited:
                    visited.add(neighbor.id)
                    next_level.append(neighbor)
                    results.append(mgp.Record(neighbor=neighbor, depth=depth))
        current_level = next_level

    return results


@mgp.write_proc
def merge_nodes(
    ctx: mgp.ProcCtx,
    node1: mgp.Vertex,
    node2: mgp.Vertex
) -> mgp.Record(merged=mgp.Vertex):
    """
    Merge two nodes, keeping node1 and transferring node2's properties/edges.
    """
    # Copy properties from node2 to node1
    for prop in node2.properties.items():
        if prop.name not in node1.properties:
            node1.properties[prop.name] = prop.value

    # Copy labels from node2
    for label in node2.labels:
        try:
            node1.add_label(label.name)
        except:
            pass  # Label already exists

    # Redirect edges
    for edge in list(node2.in_edges):
        ctx.graph.create_edge(edge.from_vertex, node1, edge.type)

    for edge in list(node2.out_edges):
        ctx.graph.create_edge(node1, edge.to_vertex, edge.type)

    # Delete node2
    ctx.graph.detach_delete_vertex(node2)

    return mgp.Record(merged=node1)


@mgp.function
def concat_labels(
    ctx: mgp.FuncCtx,
    node: mgp.Vertex,
    separator: str = ":"
) -> str:
    """Return all labels of a node concatenated."""
    return separator.join(label.name for label in node.labels)
```

## Calling Procedures in Cypher

```cypher
-- Read procedure
CALL module_name.get_neighbors(n, 2) YIELD neighbor, depth
RETURN neighbor, depth;

-- Write procedure
CALL module_name.merge_nodes(n1, n2) YIELD merged
RETURN merged;

-- Function
RETURN module_name.concat_labels(n, "-") AS labels;
```
