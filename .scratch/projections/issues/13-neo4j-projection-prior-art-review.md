# Review Neo4j GDS projection prior art

Status: ready-for-human

## Parent

`.scratch/projections/PRD.md`

## What to do

Read Neo4j's Graph Data Science projection documentation and extract lessons -
both what to adopt and what to avoid - for our `project()` / `derive()` surface
and the eventual Cypher-on-projection design.

Sources:

- Cypher projection: https://neo4j.com/docs/graph-data-science/current/management-ops/graph-creation/graph-project-cypher-projection/
- Native projection: https://neo4j.com/docs/graph-data-science/current/management-ops/graph-creation/graph-project/

Questions to answer:

- Where does their model force a full copy / materialization, and does our
  lazy read-through avoid it or repeat the mistake?
- How do they handle property selection, relationship/node filtering, and write-back?
  Which of those choices aged badly (deprecations, footguns, perf cliffs)?
- What is the ergonomic cost of their named-graph catalog vs our inline
  `project()`/`derive()` in a single query?
- Anything in their config surface we should deliberately not replicate.

## Why this is human-in-the-loop

Requires reading external docs and forming a design opinion; output feeds the
config-surface and Cypher-on-projection decisions.

## Acceptance criteria

- [x] A short comparison note: adopt / avoid / diverge, with rationale
- [x] Concrete recommendations folded into the relevant design issues

## Blocked by

None - can start immediately.

## Comments

Done. Comparison note: `.scratch/projections/prior-art-neo4j-gds.md`.

Headline findings:

- Our aggregation-function projection, node-or-handle endpoints, and `derive()`
  config keys are **convergent** with GDS's *new* (non-deprecated) Cypher
  projection - we never built the deprecated Cypher-as-strings form.
- We are **deliberately better** on the things that aged badly in GDS/APOC: lazy
  read-through vs their materialization (and no numeric-only restriction),
  functions working over virtual nodes vs APOC's `labels() == []`, and loud
  failure on ambiguity vs GDS silent first-occurrence-wins.
- **Open gaps** feeding issues 14/11: no named-graph reuse (we recompute per
  query; theirs is a catalog), no native label/type projection shorthand, and a
  decision to keep parallel-edge weighting in user Cypher (GDS lesson) rather
  than a config enum.
- For issue 11: GDS's deprecation of the string-query form is a direct warning -
  `USE projection` must be real nested Cypher, not a query string.
