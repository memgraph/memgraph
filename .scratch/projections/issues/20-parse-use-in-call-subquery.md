# Parse USE in a CALL subquery

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0004)

## What to build

Grammar and AST for `CALL { USE <expr> ... }`, binding a projection or subgraph
value as the scope's graph. Semantic analysis enforces the v1 boundary: a write
clause inside the scope (`CREATE/SET/DELETE/MERGE`) is a clear error, and a
second level of `USE` nesting is a clear error. Parsing and semantic checking
only - no execution path in this slice.

## Acceptance criteria

- [ ] `CALL { USE <expr> MATCH ... RETURN ... }` parses into the AST agreed in issue 17
- [ ] A write clause inside a `USE` scope is a clear semantic error
- [ ] A nested `USE` inside a `USE` is a clear semantic error (v1: single level)
- [ ] Standard `CALL { ... }` subqueries without `USE` are unaffected

## Blocked by

- `17-design-use-scope`
