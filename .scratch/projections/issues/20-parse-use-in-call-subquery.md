# Parse USE in a CALL subquery

Status: done - grammar/AST/visitor + `SymbolGenerator` checks; tests in `tests/unit/cypher_main_visitor.cpp`

## Parent

`.scratch/projections/PRD.md` (ADR 0004)

## What to build

Grammar and AST for `CALL { USE <expr> ... }`, binding a projection or subgraph
value as the scope's graph. Semantic analysis enforces the v1 boundary: a write
clause inside the scope (`CREATE/SET/DELETE/MERGE`) is a clear error, and a
second level of `USE` nesting is a clear error. Parsing and semantic checking
only - no execution path in this slice.

## Acceptance criteria

- [x] `CALL { USE <expr> MATCH ... RETURN ... }` parses into the AST agreed in issue 17
      - `useClause : USE expression ;` at the head of the block; `CallSubquery::use_graph_`
- [x] A write clause inside a `USE` scope is a clear semantic error
      - `SymbolGenerator` `in_use_scope` flag rejects CREATE/SET/REMOVE/DELETE/MERGE/FOREACH, inherited by nested scopes
- [x] A nested `USE` inside a `USE` is a clear semantic error (v1: single level)
- [x] Standard `CALL { ... }` subqueries without `USE` are unaffected - 198 `query_semantic` tests green

## Note

The grammar places `useClause` just after `{` (matching the ADR example
`CALL { USE g MATCH ... }`), not between `CALL` and `{`. The issue-17 design note
has been corrected to match.

## Blocked by

- `17-design-use-scope`
