# Global read-only: honor across failover + UX (message reword & snapshot)

## Parent

`specs/read_only/global-read-only-ha.md`

## What to build

Two related finishing pieces that both build on read-only enforcement:

1. **Honor read-only across failover.** When `global_read_only` is enabled and the current main dies, the newly promoted main comes up read-only (non-writeable) instead of silently accepting writes.
2. **UX polish.** Give write rejections an accurate message, and confirm snapshots still work while read-only.

End-to-end behavior:
- With `global_read_only` enabled, killing the main and letting the coordinator fail over results in a new main that rejects write queries; with it disabled, failover behaves as today (new main is writeable).
- A write query rejected because the cluster is read-only returns a message that honestly covers both causes ("cluster is in read-only mode" and "a new main is being set up") — no longer implying only a failover is in progress.
- `CREATE SNAPSHOT` succeeds while the cluster is in read-only mode.

Implementation notes (from the resolved design in the parent PRD):

Failover:
- Bump `PromoteToMainReq` to v2: the current shape becomes the v1 payload, and v2 adds a `writing_enabled` boolean, with the established `Upgrade`/`Downgrade` compatibility functions.
- The promote handler sets the main's writing flag from the request value instead of unconditionally enabling writing.
- The coordinator computes `writing_enabled = !global_read_only` at every promote call site.
- The legacy/mixed-version default (flag absent) is `writing_enabled = true`, preserving today's behavior; read-only is best-effort during a rolling upgrade and self-heals once all nodes are upgraded.
- Depends on the gated promote trigger from `02-enforce-on-running-main.md` — without it, a read-only promoted main (writing off) would be re-promoted every reconciliation cycle.

UX:
- Reword the shared `WriteQueryOnMainException` message to neutral text fitting both causes, since a data instance cannot distinguish operator-driven read-only from the transient post-promotion window under the single-bit design. Keep a stable, assertable prefix.
- Update the existing `disable_writing_on_main_after_restart.py` e2e assertion to match the reworded message.
- `CREATE SNAPSHOT` needs no production change — it is a non-write query (`RWType::NONE`) that arranges its own storage access — so this is e2e coverage only.

## Acceptance criteria

- [ ] With `global_read_only` enabled, a failover promotes a new main that rejects writes.
- [ ] With `global_read_only` disabled, a failover promotes a writeable new main (unchanged behavior).
- [ ] An old-version data instance receiving a downgraded (v1) promote request defaults to writeable.
- [ ] `WriteQueryOnMainException` message is reworded to neutral text covering both causes, with a stable prefix.
- [ ] The existing `disable_writing_on_main_after_restart.py` assertion is updated and passes.
- [ ] Unit test: `PromoteToMainReq` v2 SLK round-trip + `Upgrade`/`Downgrade` compatibility with v1.
- [ ] E2E test: enable read-only, kill the main, assert the newly promoted main is read-only.
- [ ] E2E test: a write under read-only is rejected with the reworded message.
- [ ] E2E test: `CREATE SNAPSHOT` succeeds while the cluster is in read-only mode.

## Blocked by

- `02-enforce-on-running-main.md`
