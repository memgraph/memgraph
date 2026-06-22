# Label-filtered scan over a projection in a USE scope

Status: done - `LabelsTest` evaluates over a `VirtualNode` in `src/query/interpret/eval.hpp`; the index, edge-index, and join rewriters leave the `USE` body a full `ScanAll` + `Filter` (`src/query/plan/rewrite/`). Full-scan semantics recorded in ADR 0004. Tests in `tests/unit/interpreter.cpp` and `tests/e2e/write_procedures/virtual_graph.py`.

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

`MATCH (n:Label)` inside a `USE` scope returns the projection nodes carrying that
label, via a documented full-scan filter (a projection has no label index). A
property predicate in the `WHERE` also evaluates by scan.

## Acceptance criteria

- [x] `MATCH (n:Label)` returns only the projection nodes with that label
- [x] A property predicate filters correctly (full scan, no index)
- [x] Full-scan semantics for projection scans are documented (no index/stats)
- [x] e2e test

## Blocked by

- `21-scan-projection-nodes-in-use`
