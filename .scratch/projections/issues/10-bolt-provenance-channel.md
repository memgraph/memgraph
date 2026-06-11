# Bolt provenance channel - per-node ref + RUN-header schema table

Status: done

## Parent

`.scratch/projections/PRD.md`

## What to build

Convey per-property provenance to Lab without breaking existing clients. Each
projected node carries a small reserved property - a **projection tag** (e.g.
`__mg_overlay_ref: 0`, a plain Int generic clients ignore) - referencing a
**projection-schema table** sent once in the **RUN header** alongside `fields`
(not the trailing summary, so streaming clients resolve provenance as nodes
arrive). The table entry is a small map (e.g. `{overlay: [...], hidden: [...],
edgeType: "..."}`) so Lab styling can grow without a wire change. Property
values are never wrapped - provenance is schema, not per-value tags. Relies on
the static schema. Follow-on - not MVP-blocking.

## Acceptance criteria

- [x] Each projected node carries a reserved projection-tag property referencing its schema entry
- [x] The projection-schema table is emitted once in the RUN header, before any record
- [x] The table entry lists at least the overlay keys for that projection (hidden dropped from the wire per the locked design - a hidden key is already omitted from the property map, so its name carries no styling value)
- [x] A node from no projection carries no tag and is treated as fully real
- [x] An existing driver decodes the result unchanged (reserved key is an ordinary Int property; unknown header key ignored)
- [x] Scalar property values are sent unwrapped
- [x] Test asserting the header table, per-node tag, and unchanged decoding for a generic client (e2e for the per-node tag; bolt encoder/decoder round-trip for the table shape and unwrapped scalars)

## Blocked by

- `09-bolt-overlay-element-id`

## Design (locked, ready to implement)

Decisions taken with the user before implementation:

- **Drop `hidden` from the wire.** A hidden key is already omitted from the serialized
  property map, so the client has no value to style or annotate; listing hidden key names
  would only leak the names of deliberately-suppressed properties for zero benefit. Wire
  schema entry is `{overlay: [<key names>], edgeType: "<type>"}`. The table is an
  extensible map, so `hidden` is a non-breaking addition later if a real need appears
  (e.g. Lab suppressing those keys when a user clicks through to the real node).
- **Table shape.** A map under a reserved RUN-metadata key keyed by the schema ref:
  `projection_schema: { "<ref>": {overlay:[...], edgeType:"..."} }`. (Bolt map keys are
  strings, so the loose `{0:{...}}` in the issue body becomes `{"<ref>":{...}}`.)
- **The schema ref is the `derive()` output symbol's position** (`Aggregate::Element::
  output_sym.position()`), not a freshly-assigned index. It is plan-global, unique per
  `derive()` site, immutable, and already in scope at both prepare time and execution -
  so prepare-time table-building and execution-time node-tagging agree with no counter,
  no plan mutation (cached plans are shared across threads - must not mutate at prepare).
  The per-node tag `__mg_overlay_ref` carries this same int; all overlay nodes from one
  `derive()` share it (one table entry).
- **Graceful degrade for non-literal options.** If a `derive()`'s options/propertyPolicy
  aren't a static map literal (e.g. the whole options map is a `$param`), the schema is
  unknowable pre-execution: that projection emits no tag and no table entry. Nodes still
  serialize at origin identity (issue 09); only the styling metadata is absent. No error.

Why prepare-time extraction is needed: the RUN SUCCESS header (with `fields`) is built at
prepare time, before execution, but the schema is currently determined at execution time
in the `DERIVE` aggregation. So the header table must be extracted statically from the
plan/AST at prepare time.

### Two-commit plan

**Commit A - per-node tag** (self-contained, Python-testable):
- `src/query/virtual_node.hpp`: add `int64_t projection_ref_` (in `Impl`, default
  `kNoProjectionRef = -1`), ctor param, `HasProjectionRef()`/`ProjectionRef()` accessors.
- `src/query/plan/operator.cpp`: the two `DERIVE` cases (~6621, ~6688) pass
  `agg_elem.output_sym.position()` into `ProjectPathWithOptions`, which threads it to
  `BuildDerivedNode` and onto the `VirtualNode`.
- `src/glue/communication.cpp` `ToBoltVertex(const query::VirtualNode &)`: if
  `HasProjectionRef()`, add `properties["__mg_overlay_ref"] = Value{ProjectionRef()}`.
- e2e (`tests/e2e/write_procedures/virtual_graph.py`): overlay nodes from `derive()`
  carry `__mg_overlay_ref` (an int) in `node.properties`, all sharing one value; real
  nodes (plain MATCH) and synthetic `virtualNode()` nodes carry none; scalar props arrive
  unwrapped; the query decodes normally for the generic client.

**Commit B - RUN-header table** (needs a C++ seam; pymgclient exposes only `fields`):
- New query-layer struct (no bolt deps), e.g. `query/projection_schema.hpp`:
  `struct ProjectionSchema { int64_t ref; std::vector<std::string> overlay; std::string
  edge_type; };`
- Prepare-time extractor: a `HierarchicalLogicalOperatorVisitor` overriding
  `PreVisit(Aggregate&)` that, for each `DERIVE` element, reads `output_sym.position()`
  (ref) and statically extracts from `element.arg2` (a `MapLiteral`): `virtualEdgeType`
  (a `PrimitiveLiteral` string), `propertyPolicy` overlay keys (`MapLiteral` entries whose
  `PrimitiveLiteral` value == "overlay"), and `sourceNodeProperties`/`targetNodeProperties`
  keys (overlay-bound by override). `Downcast<MapLiteral>/<PrimitiveLiteral>`;
  `PrimitiveLiteral::value_` is `ExternalPropertyValue` with `IsString()/ValueString()`;
  `MapLiteral::elements_` is `unordered_map<PropertyIx, Expression*>` (`PropertyIx::name`).
- Hook it in `PrepareCypherQuery` (`src/query/interpreter.cpp` ~3547, where `header` is
  built from `plan->plan().OutputSymbols(...)`). Store on a new
  `PreparedQuery::projection_schemas` and `Interpreter::PrepareResult::projection_schemas`.
- `src/glue/SessionHL.cpp::InterpretPrepare` (~428) returns the schemas alongside
  `{headers, qid}`; convert to a bolt `map_t` and add under `projection_schema` in the RUN
  SUCCESS metadata in `src/communication/bolt/v1/states/handlers.hpp` `HandlePrepare`
  (~212, where `fields` is emitted).
- Test seam: pymgclient can't read RUN metadata. Use a C++ test - either assert
  `PreparedQuery::projection_schemas` for a `derive()` query at the interpreter unit seam
  (`tests/unit/interpreter.cpp`), or an encoder/decoder round-trip
  (`tests/unit/bolt_encoder.cpp` + `bolt_decoder.cpp`) for the `map_t` + the tagged
  `Vertex`. The C++ `communication::bolt::Client` (`QueryData{fields, records, metadata}`)
  is the fullest seam if an integration test is wanted.

### Key seams (verified)
- RUN header emit: `src/communication/bolt/v1/states/handlers.hpp` `HandlePrepare` (~212).
- Cypher header build / plan available: `src/query/interpreter.cpp` `PrepareCypherQuery`
  (~3547).
- `InterpretPrepare` returns `{headers, qid}`: `src/glue/SessionHL.cpp` (~428).
- DERIVE execution (element in scope): `src/query/plan/operator.cpp` (~6621, ~6688);
  `BuildDerivedNode`/`ProjectPathWithOptions` (~6736, ~6813).
- `__mg_*` reserved-key precedent (Enum): `src/communication/bolt/v1/mg_types.hpp` (~29).
- Per-node tag is a plain Int property, so pymgclient sees it via `node.properties`; only
  the RUN-header table is invisible to pymgclient.
