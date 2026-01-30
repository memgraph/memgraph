---
name: memgraph-apoc-to-mage-translation
description: Translate Neo4j APOC procedures to Memgraph MAGE query modules. Use when the user wants to port APOC procedures, convert Neo4j code to Memgraph, migrate from Neo4j to Memgraph, or implement Neo4j-compatible procedures in MAGE.
---

# Neo4j APOC to Memgraph MAGE Translation

This skill helps translate Neo4j APOC (Awesome Procedures on Cypher) procedures into Memgraph MAGE (Memgraph Advanced Graph Extensions) query modules.

## Workflow

### Step 1: Get the Neo4j Documentation URL

Ask the user for the Neo4j APOC/Neosemantics documentation URL containing the procedures to translate.

Example URLs:
- `https://neo4j.com/labs/neosemantics/5.14/import/`
- `https://neo4j.com/labs/apoc/5/overview/apoc.convert/`

### Step 2: Ask for Module and Function Naming Preferences

Before implementing, ask the user:

1. **Module name**: What should the Memgraph module be called?
   - Example: `n10s.rdf.import.fetch` → user might want `rdf_util` or `rdf_import` or `neosemantics`

2. **Function naming style**: Confirm snake_case convention or any preferences
   - Default: `fetchData` → `fetch_data`
   - User might want: `fetch`, `import_rdf`, etc.

3. **Grouping**: Should related procedures be in one module or separate?
   - Example: `n10s.rdf.import.fetch` and `n10s.rdf.import.inline` → both in `rdf_util.py`?

**Example questions to ask:**

```
I found these procedures to translate:
- n10s.rdf.import.fetch
- n10s.rdf.import.inline

How would you like to name these in Memgraph?

1. Module name (file will be mage/python/<name>.py):
   - Suggested: rdf_util, rdf_import, or mg_semantics

2. Function names:
   - n10s.rdf.import.fetch → fetch? import_fetch? rdf_fetch?
   - n10s.rdf.import.inline → inline? import_inline? rdf_inline?

The final Cypher calls would be: <module>.<function>(...)
```

### Step 3: Scrape and Analyze Procedures

Use the `WebFetch` tool to fetch the documentation page. Extract:

1. **Procedure signatures** - name, parameters, return types
2. **Descriptions** - what each procedure does
3. **Example usage** - Cypher calls and expected results
4. **Parameter details** - types, defaults, constraints

### Step 4: Translate Each Procedure

For each procedure, create the Memgraph MAGE equivalent following the patterns below.

### Step 5: Add Alias Mapping (IMPORTANT)

**Memgraph only supports ONE dot in function/procedure names** (e.g., `module.function`).

Neo4j APOC uses multiple dots (e.g., `apoc.map.flatten`). To support both syntaxes, add an alias mapping to `config/mappings.json`:

```json
{
    "apoc.map.flatten": "map.flatten",
    "apoc.convert.toJson": "json_util.to_json"
}
```

This allows users to call either:
- `CALL apoc.map.flatten(...)` (Neo4j syntax - via alias)
- `CALL map.flatten(...)` (Memgraph native syntax)

### Step 6: Add Dependencies (if needed)

If your module requires external Python packages (e.g., `rdflib` for RDF parsing, `requests` for HTTP calls), add them to `mage/python/requirements.txt`:

```
mage/python/requirements.txt
```

**Important:**
- Use specific version pins (e.g., `rdflib==7.1.1`, not just `rdflib`)
- Check if the dependency is already in the file before adding
- Prefer using the package manager to find the latest stable version

### Step 7: Create End-to-End Tests

Create tests in `mage/tests/e2e/<module>_test/` using:
- Examples from the Neo4j documentation as test cases
- Edge cases (empty inputs, null values)
- Error cases (invalid inputs, expected exceptions)

See the [Testing (Required)](#testing-required) section below for detailed format.

## Naming Conventions

### Critical: Single Dot Limitation

Memgraph only allows **one dot** in procedure/function names. The pattern is:

```
module_name.function_name
```

**NOT** `apoc.module.function` (multiple dots - Neo4j style).

### Naming Transformation Rules

| Neo4j APOC | Memgraph MAGE | Notes |
|------------|---------------|-------|
| `apoc.map.flatten` | `map.flatten` | Drop `apoc.` prefix |
| `apoc.coll.containsAll` | `collections.contains_all` | camelCase → snake_case |
| `apoc.convert.toJson` | `json_util.to_json` | Module name may differ |
| `apoc.refactor.cloneNodes` | `refactor.clone_nodes` | camelCase → snake_case |

### Python Function Names

In MAGE Python modules, use underscores for multi-word function names:

```python
# File: mage/python/map.py

@mgp.function
def flatten(ctx: mgp.FuncCtx, map_value: mgp.Map) -> mgp.Map:
    """Called as: map.flatten(...)"""
    pass

@mgp.function
def from_lists(ctx: mgp.FuncCtx, keys: list, values: list) -> mgp.Map:
    """Called as: map.from_lists(...)"""
    pass
```

### The Alias Mappings File

The file `config/mappings.json` maps Neo4j APOC names to Memgraph equivalents:

```json
{
    "apoc.map.flatten": "map.flatten",
    "apoc.map.fromLists": "map.from_lists",
    "apoc.coll.containsAll": "collections.contains_all",
    "apoc.convert.toJson": "json_util.to_json",
    "apoc.refactor.mergeNodes": "refactor.merge_nodes"
}
```

**After implementing a new procedure, always add its mapping to this file.**

## Translation Patterns

### Decorator Mapping

| Neo4j APOC | Memgraph MAGE |
|------------|---------------|
| Read procedure | `@mgp.read_proc` |
| Write procedure | `@mgp.write_proc` |
| User function | `@mgp.function` |

### Type Mapping

| Neo4j Type | Memgraph Type |
|------------|---------------|
| `STRING` | `str` |
| `INTEGER` | `int` |
| `FLOAT` | `float` |
| `BOOLEAN` | `bool` |
| `NODE` | `mgp.Vertex` |
| `RELATIONSHIP` | `mgp.Edge` |
| `PATH` | `mgp.Path` |
| `MAP` | `mgp.Map` (or `dict`) |
| `LIST OF X` | `mgp.List[X]` |
| `ANY` | `mgp.Any` |
| `NULL` allowed | `mgp.Nullable[X]` |

### Procedure Template

```python
import mgp
from typing import List, Dict, Any, Union

@mgp.read_proc
def procedure_name(
    ctx: mgp.ProcCtx,
    required_param: str,
    optional_param: mgp.Nullable[str] = None
) -> mgp.Record(field1=str, field2=int):
    """
    Brief description of what this procedure does.

    Parameters
    ----------
    required_param : str
        Description of required parameter.
    optional_param : str, optional
        Description of optional parameter.

    Returns
    -------
    mgp.Record
        field1: Description
        field2: Description
    """
    # Implementation
    result_field1 = "value"
    result_field2 = 42

    return mgp.Record(field1=result_field1, field2=result_field2)
```

### Write Procedure Template (for graph modifications)

```python
@mgp.write_proc
def create_something(
    ctx: mgp.ProcCtx,
    properties: mgp.Map
) -> mgp.Record(node=mgp.Vertex):
    """Creates a new node with given properties."""
    vertex = ctx.graph.create_vertex()
    for key, value in properties.items():
        vertex.properties[key] = value
    return mgp.Record(node=vertex)
```

### Function Template

```python
@mgp.function
def utility_function(
    ctx: mgp.FuncCtx,
    input_value: str
) -> str:
    """Returns transformed input value."""
    return input_value.upper()
```

## Key API Differences

### Graph Access

| Neo4j APOC | Memgraph MAGE |
|------------|---------------|
| `db.labels()` | Iterate `ctx.graph.vertices` and collect labels |
| `db.relationshipTypes()` | Iterate edges and collect types |
| Transaction management | Handled automatically by `@mgp.write_proc` |

### Node/Vertex Operations

```python
# Get vertex by ID
vertex = ctx.graph.get_vertex_by_id(vertex_id)

# Create vertex
vertex = ctx.graph.create_vertex()
vertex.add_label("LabelName")
vertex.properties["key"] = "value"

# Iterate all vertices
for vertex in ctx.graph.vertices:
    labels = [label.name for label in vertex.labels]
    props = dict(vertex.properties.items())
```

### Edge Operations

```python
# Create edge
edge = ctx.graph.create_edge(from_vertex, to_vertex, mgp.EdgeType("TYPE"))
edge.properties["key"] = "value"

# Access edge properties
for edge in vertex.out_edges:
    edge_type = edge.type.name
    start = edge.from_vertex
    end = edge.to_vertex
```

### Returning Multiple Records

```python
@mgp.read_proc
def get_all_nodes(ctx: mgp.ProcCtx) -> mgp.Record(node=mgp.Vertex):
    """Returns all nodes one by one."""
    return [mgp.Record(node=Vertex(v)) for v in ctx.graph.vertices]
```

## Common Translations

### apoc.create.node → MAGE equivalent

```python
@mgp.write_proc
def node(
    ctx: mgp.ProcCtx,
    labels: mgp.List[str],
    properties: mgp.Map
) -> mgp.Record(node=mgp.Vertex):
    """Create a node with given labels and properties."""
    vertex = ctx.graph.create_vertex()
    for label in labels:
        vertex.add_label(label)
    for key, value in properties.items():
        vertex.properties[key] = value
    return mgp.Record(node=vertex)
```

### apoc.create.relationship → MAGE equivalent

```python
@mgp.write_proc
def relationship(
    ctx: mgp.ProcCtx,
    from_node: mgp.Vertex,
    rel_type: str,
    properties: mgp.Map,
    to_node: mgp.Vertex
) -> mgp.Record(rel=mgp.Edge):
    """Create a relationship between two nodes."""
    edge = ctx.graph.create_edge(
        from_node,
        to_node,
        mgp.EdgeType(rel_type)
    )
    for key, value in properties.items():
        edge.properties[key] = value
    return mgp.Record(rel=edge)
```

## File Structure

Place new MAGE modules in `mage/python/` with the pattern:

```
mage/python/
├── module_name.py          # Main module implementation
└── mage/
    └── module_name_util/   # Optional: helper utilities
        └── helpers.py
```

**After creating the module, update these files as needed:**

```
config/
└── mappings.json           # Add Neo4j → Memgraph name mappings

mage/python/
└── requirements.txt        # Add new Python dependencies (if any)
```

### Complete Workflow Example

1. **Create module:** `mage/python/rdf_util.py`
2. **Define function:** `def fetch(...)` → callable as `rdf_util.fetch(...)`
3. **Add dependencies to `mage/python/requirements.txt`** (if needed):
   ```
   rdflib==7.1.1
   requests==2.32.3
   ```
4. **Add mapping to `config/mappings.json`:**
   ```json
   {
       "n10s.rdf.import.fetch": "rdf_util.fetch"
   }
   ```
5. Now both work:
   - `CALL n10s.rdf.import.fetch(...)` (Neo4j syntax)
   - `CALL rdf_util.fetch(...)` (Memgraph syntax)

## Module Header Template

```python
"""
Module: module_name
Description: Brief description of the module purpose.

Translated from Neo4j APOC: [original procedure names]
Documentation: [link to original Neo4j docs]
"""
import mgp
from typing import List, Dict, Any, Optional, Union
```

## Testing (Required)

After implementing a procedure, create end-to-end tests in `mage/tests/e2e/`.

### Test Directory Structure

```
mage/tests/e2e/<module>_test/
├── test_<feature>_<case>/
│   ├── input.cyp      # Setup data (can be empty)
│   └── test.yml       # Test definition
├── test_<feature>_<case2>/
│   ├── input.cyp
│   └── test.yml
```

### input.cyp - Setup Data

Contains Cypher queries to set up test data. Can be empty if no setup needed.

```cypher
CREATE (n1:Person {name: 'Alice', age: 30, city: 'New York'});
CREATE (n2:Person {name: 'Bob', age: 25, country: 'USA'});
```

### test.yml - Test Definition

**Basic test with expected output:**

```yaml
query: >
  RETURN map.flatten({person: {name: "John", city: "NYC"}, x: 1}) AS result;

output:
  - result: {"person.city": "NYC", "person.name": "John", "x": 1}
```

**Test with setup data:**

```yaml
query: >
  MATCH (n1:Person {name: 'Alice'}), (n2:Person {name: 'Bob'})
  CALL refactor.merge_nodes([n1, n2], {properties: 'combine'}) YIELD node
  RETURN node.name as name, node.age as age;

output:
  - name: [Alice, Bob]
    age: [30, 25]
```

**Test for expected error (negative test):**

```yaml
query: >
  CALL refactor.merge_nodes([], {properties: 'combine'}) YIELD node
  RETURN node;

exception: >-
  "The nodes list cannot be empty"
```

**Test with path/graph results:**

```yaml
query: >
  MATCH (d:Dog) CALL path.expand(d, ["HUNTS>"], [], 1, 3) YIELD result
  RETURN result;

output:
  - result:
      nodes:
        - labels:
            - Dog
          properties:
            name: Rex
        - labels:
            - Cat
          properties:
            name: Tom
      relationships:
        - label: HUNTS
          properties: {}
```

### Deriving Tests from Neo4j Documentation

**Use examples from Neo4j APOC documentation as test cases.** When scraping the documentation:

1. Extract example Cypher queries
2. Extract expected outputs
3. Translate to Memgraph syntax
4. Create test files

**Example: Translating Neo4j doc example to test**

Neo4j documentation shows:
```cypher
RETURN apoc.map.flatten({person: {name: "John"}}) AS result
-- Returns: {person.name: "John"}
```

Create test:
```
mage/tests/e2e/map_test/test_flatten_nested/
├── input.cyp   (empty)
└── test.yml
```

```yaml
# test.yml
query: >
  RETURN map.flatten({person: {name: "John"}}) AS result;

output:
  - result: {"person.name": "John"}
```

### Test Naming Convention

```
test_<function>_<scenario>
```

Examples:
- `test_flatten_nested` - Test flatten with nested maps
- `test_flatten_empty` - Test flatten with empty map
- `test_merge_nodes_combine_basic` - Test merge_nodes with combine strategy
- `test_merge_nodes_invalid_empty` - Test error case with empty input

### Minimum Test Coverage

For each translated procedure, create tests for:

1. **Happy path** - Basic successful usage
2. **Edge cases** - Empty inputs, null values, large data
3. **Error cases** - Invalid inputs, expected exceptions
4. **Documentation examples** - All examples from Neo4j docs

## Additional Resources

- For complete mgp API details, see [references/mgp_api_reference.md](references/mgp_api_reference.md)
- For concrete translation examples, see [references/examples.md](references/examples.md)
- Example implementations in codebase: `mage/python/import_util.py`
