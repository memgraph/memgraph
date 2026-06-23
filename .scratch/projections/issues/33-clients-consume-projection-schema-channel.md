# mgconsole and Lab consume the projection-schema channel

Status: not started

## Parent

`.scratch/projections/PRD.md` (issues 09, 10 - done)

## What to build

The Bolt provenance channel ships (issue 10): every overlay node a `derive()`
produces carries a reserved `__mg_overlay_ref` Int property keyed to a
`projection_schema` table in the RUN header. No client consumes it yet, so the
only visible effect today is that the raw tag leaks into generic clients'
property maps - e.g. `mgconsole` prints `__mg_overlay_ref: 4` as if it were user
data.

This is left in deliberately as a standing reminder that the clients are not yet
projection-aware. The work is to close that gap:

- **mgconsole**: stop displaying the reserved `__mg_overlay_ref` key in node
  output (it is provenance, not user data). Minimally, suppress the `__mg_*`
  reserved keys; ideally, read the RUN-header `projection_schema` table and
  annotate overlay properties.
- **Lab**: read the `projection_schema` RUN-header table, join overlay nodes to
  their entry via `__mg_overlay_ref`, and style computed-overlay properties
  distinctly from read-through real values. Use the synthetic edge type from the
  table entry. Do not surface the reserved key as a normal property.

The wire contract is fixed and non-breaking: the per-node tag is a plain Int,
the header table is keyed by the ref (the `derive()` output symbol position),
and the table entry is an extensible map (`{overlay: [...], edgeType: "..."}`).
`hidden` keys are intentionally absent from the wire.

## Acceptance criteria

- [ ] mgconsole no longer shows `__mg_overlay_ref` (and other `__mg_*` reserved
      keys) as ordinary node properties.
- [ ] Lab resolves an overlay node to its `projection_schema` entry via the tag
      and styles overlay properties distinctly from read-through values.
- [ ] Generic third-party drivers are unaffected (no wire change).

## Notes

If neither client is going to consume this soon, the alternative is to drop the
per-node tag until there is a consumer: origin-identity serialization (issue 09)
already preserves click-through, and the RUN-header table is invisible to
generic clients, so only the per-node property pollutes dumb output. Keeping it
in is the chosen path - the leak is the reminder.
