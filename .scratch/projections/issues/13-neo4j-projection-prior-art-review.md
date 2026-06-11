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

- [ ] A short comparison note: adopt / avoid / diverge, with rationale
- [ ] Concrete recommendations folded into the relevant design issues

## Blocked by

None - can start immediately.
