# Global read-only: coordinator setting global_read_only (persisted & shown)

## Parent

`specs/read_only/global-read-only-ha.md`

## What to build

Add a new coordinator setting `global_read_only` (boolean) end-to-end through the coordinator, so an operator can set it and see it, and it survives coordinator restarts and re-elections. This slice introduces the persisted source-of-truth field only — it does **not** yet affect whether writes are accepted (that comes in a follow-up slice).

End-to-end behavior:
- `SET COORDINATOR SETTING "global_read_only" TO "true"` / `"false"` is accepted on the coordinator leader and appended to the Raft log.
- An invalid value (not `"true"`/`"false"`) is rejected with a clear error.
- `SHOW COORDINATOR SETTINGS` includes a `global_read_only` row reflecting the current value.
- The value is persisted in the coordinator's Raft-replicated cluster state and restored after a coordinator restart.

Implementation notes (from the resolved design in the parent PRD):
- Thread `global_read_only` through `CoordinatorClusterState` + `CoordinatorClusterStateDelta`: member (default `false`), getter/setter, all copy/move special members, `operator==`, `DoAction`, `to_json`/`from_json`, and a new name constant.
- `from_json` must default `global_read_only` to `false` when the key is absent (backward compatibility with clusters serialized before this feature).
- Add the field to `SerializeUpdateClusterState` and `DecodeLog` in the coordinator state machine, and add a getter forwarder through the Raft-state layer.
- Add `global_read_only` to the coordinator-setting whitelist + value-parse branch, and add a row to the `SHOW COORDINATOR SETTINGS` output.
- No grammar/AST change is needed — `SET`/`SHOW COORDINATOR SETTING(S)` are generic name/value plumbing.

## Acceptance criteria

- [ ] `SET COORDINATOR SETTING "global_read_only" TO "true"|"false"` succeeds on the leader and is rejected for invalid values.
- [ ] `SHOW COORDINATOR SETTINGS` shows the current `global_read_only` value.
- [ ] The value persists across a coordinator restart / leader re-election.
- [ ] `from_json` defaults `global_read_only` to `false` when the key is absent.
- [ ] Unit test: `CoordinatorClusterState` serialize/deserialize round-trip including `global_read_only`, `DoAction` applies the delta, and backward-compat default is verified.
- [ ] Unit test: the `SET`/`SHOW` coordinator-setting path for `global_read_only` (accept, reject invalid, show).

## Blocked by

None - can start immediately.
