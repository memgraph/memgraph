# Bolt provenance channel - per-node ref + RUN-header schema table

Status: ready-for-agent

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

- [ ] Each projected node carries a reserved projection-tag property referencing its schema entry
- [ ] The projection-schema table is emitted once in the RUN header, before any record
- [ ] The table entry lists at least the overlay and hidden keys for that projection
- [ ] A node from no projection carries no tag and is treated as fully real
- [ ] An existing driver decodes the result unchanged (reserved key is an ordinary Int property; unknown header key ignored)
- [ ] Scalar property values are sent unwrapped
- [ ] Test asserting the header table, per-node tag, and unchanged decoding for a generic client

## Blocked by

- `09-bolt-overlay-element-id`
