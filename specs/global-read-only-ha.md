# PRD: Global read-only mode across the HA cluster

## Problem Statement

As an operator of a Memgraph HA cluster, I have no way to place the entire cluster into a read-only state. Today writes are only ever rejected implicitly: replicas reject all writes, and a freshly promoted main is briefly non-writeable during the failover window. There is no operator-controlled, cluster-wide, persistent "read-only" switch. This makes it impossible to safely perform maintenance windows, take consistent backups without racing new writes, or freeze the dataset while investigating an issue — without shutting the cluster down.

## Solution

Introduce a cluster-wide **global read-only mode**, controlled by the operator through the coordinator:

- `SET COORDINATOR SETTING "global_read_only" TO "true"` puts the whole cluster into read-only mode — the current main stops accepting write queries while continuing to serve reads and replicate existing data.
- `SET COORDINATOR SETTING "global_read_only" TO "false"` returns the cluster to normal read/write operation.
- `SHOW COORDINATOR SETTINGS` reflects the current value.

The setting is persisted in the coordinator's Raft-replicated cluster state, so it survives coordinator restarts and re-elections, and is honored across failovers (a newly promoted main comes up read-only if the cluster is in read-only mode). Read-only mode blocks all write sources on the main (user Cypher writes, TTL background expiry, stream-driven writes, trigger-driven writes) but leaves reads, replication, and `CREATE SNAPSHOT` working.

## User Stories

1. As a cluster operator, I want to enable read-only mode for the whole cluster with a single coordinator query, so that I can freeze all writes during a maintenance window.
2. As a cluster operator, I want to disable read-only mode with a single coordinator query, so that the cluster resumes accepting writes when maintenance is complete.
3. As a cluster operator, I want `SHOW COORDINATOR SETTINGS` to display the current `global_read_only` value, so that I can confirm the cluster's state before and after toggling it.
4. As a cluster operator, I want the read-only setting to be persisted in the coordinator cluster state, so that it survives a coordinator restart or leader re-election.
5. As a cluster operator, I want the read-only setting to be honored after a failover, so that a newly promoted main does not silently start accepting writes while the cluster is meant to be read-only.
6. As an application developer, I want write queries against a read-only cluster to fail with a clear error message, so that I understand the cluster is intentionally read-only rather than mid-failover.
7. As an application developer, I want read queries to keep working while the cluster is read-only, so that my read workloads are unaffected by a maintenance window.
8. As a cluster operator, I want to take a snapshot (`CREATE SNAPSHOT`) while the cluster is in read-only mode, so that I can capture a consistent backup of the frozen dataset.
9. As a cluster operator, I want replication to keep running while the cluster is read-only, so that replicas stay in sync with the (unchanging) main and remain ready for failover.
10. As a cluster operator, I want TTL-based background deletions to pause while the cluster is read-only, so that the dataset truly does not change during the window.
11. As a cluster operator, I want stream-ingested and trigger-driven writes to also be rejected while the cluster is read-only, so that read-only means "no writes from any source."
12. As a cluster operator, I want enabling read-only mode on an already-running main to take effect without me restarting or re-promoting anything, so that toggling is a lightweight, online operation.
13. As a cluster operator, I want a transient RPC failure while toggling read-only mode to be self-healed by the coordinator, so that the setting is not silently dropped and the cluster eventually converges to the requested state.
14. As a cluster operator upgrading my cluster version-by-version, I want the feature to degrade gracefully during a mixed-version window, so that older instances behave as before and the setting is honored best-effort until the upgrade completes.
15. As a cluster operator on an existing (pre-feature) cluster, I want the default value of `global_read_only` to be false after upgrade, so that upgrading does not unexpectedly freeze my cluster.
16. As a Memgraph maintainer, I want the new cluster-state field to round-trip correctly through serialization and Raft log encode/decode, so that read-only state is not lost or corrupted across coordinator restarts.
17. As a Memgraph maintainer, I want the promote and config-update RPCs to remain backward/forward compatible across versions, so that rolling upgrades do not break the coordinator↔data-instance protocol.
18. As a cluster operator, I want an invalid value for `global_read_only` to be rejected with a clear error, so that typos do not silently misconfigure the cluster.
19. As a cluster operator, I want the setting to only be settable on the coordinator leader, so that the change goes through Raft consensus and is consistent cluster-wide.
20. As a cluster operator, I want replicas to never be affected by a stray "disable writing" instruction, so that the coordinator never crashes a replica by treating it as a main.

## Implementation Decisions

**Core principle — single-bit enforcement.** Read-only enforcement reuses the existing `RoleMainData::writing_enabled_` flag on the data instance. The coordinator's new persistent `global_read_only` setting is the source of truth; the main's `writing_enabled_` is its projection. Replicas already reject all writes, and the interpreter's write gate, TTL user-check, and stream/trigger execution all already key off `IsMainWriteable()`, so no per-write-source changes are needed. No second "read-only" boolean is introduced (the transient post-promotion non-writeable window and operator-driven read-only share the same bit; a data instance cannot distinguish them, which is accepted).

**Module: `CoordinatorClusterState` + `CoordinatorClusterStateDelta`.** Add a `global_read_only` boolean field, threaded through: member (default `false`), getter/setter, all copy/move special members, `operator==`, `DoAction`, `to_json`/`from_json`, and a `global_read_only` name constant. `from_json` must default to `false` for backward compatibility with clusters serialized before this feature.

**Module: coordinator state machine + Raft state.** Add the field to `SerializeUpdateClusterState` and `DecodeLog`, and add a getter forwarder through the Raft-state layer so the reconciliation loop and `SHOW COORDINATOR SETTINGS` can read it.

**Module: coordinator setting apply/show.** Add `global_read_only` to the settings whitelist and the value-parse branch (boolean, parsed as `"true"`/`"false"`, invalid input rejected), and add a row to the `SHOW COORDINATOR SETTINGS` output. Setting the value appends to the Raft log and relies on the reconciliation loop for propagation (no one-shot push), consistent with how `deltas_batch_progress_size` is handled today. The setting is only applicable on the coordinator leader (enforced by the existing Raft append path).

**Module: `ReplicationState` writing control.** Add a `DisableWritingOnMain()` operation alongside the existing `EnableWritingOnMain()`, so the data instance can be moved back to non-writeable at runtime. `IsMainWriteable()` semantics are unchanged.

**Module: `PromoteToMainReq` RPC — version 2.** The promote request gains a `writing_enabled` boolean. The request is versioned: the current shape becomes the v1 payload and the new shape is v2, with the established `Upgrade`/`Downgrade` compatibility functions. The promote handler sets the main's writing flag from the request value instead of unconditionally enabling writing. The coordinator computes `writing_enabled = !global_read_only` at every promote call site. The legacy/mixed-version default (when the flag is absent) is `writing_enabled = true`, preserving today's behavior; consequently read-only is best-effort during a rolling upgrade and self-heals once all nodes are upgraded.

**Module: `UpdateDataInstanceConfigRpc` — version 2.** The payload is extended from a single `deltas_batch_progress_size` value to a combined payload `{ deltas_batch_progress_size, disable_writing }`, versioned with a `Downgrade()` that drops `disable_writing`. The two config items now travel together: every send carries both desired values. No `StateCheck`/`InstanceState` change is required — the data instance already reports `is_writing_enabled`. The handler applies `deltas_batch_progress_size` unconditionally, and applies the writing flag (`writing_enabled = !disable_writing`) **only when the instance is currently main** — a hard `IsMain()` guard, because accessing the main-role state on a replica asserts/crashes.

**Module: coordinator reconciliation loop.** Two changes:
1. Gate the existing "re-promote a non-writeable main" trigger so it only fires when the cluster is **not** read-only. Conceptually the trigger changes from `!is_writing_enabled` to `(!is_writing_enabled AND NOT global_read_only)`, so a deliberately read-only main is not re-promoted every cycle. This branch continues to cover restart recovery and the "clear read-only → re-enable writing" direction via the existing idempotent promotion path (which also re-registers replicas).
2. Add a reconciliation block in the current-main branch: if the reported `is_writing_enabled` diverges from `!global_read_only`, send `UpdateDataInstanceConfigRpc` with `disable_writing = global_read_only`. In practice this drives the disable direction. This is declarative and self-healing across failed RPCs, main restarts, and re-elections.

**Module: write-rejection message.** Reword the shared `WriteQueryOnMainException` message to neutral text that fits both causes ("cluster is in read-only mode" and "a new main is being set up"), since the data instance cannot distinguish them under the single-bit design.

**Enforcement scope (no code change).** `CREATE SNAPSHOT` is classified as a non-write query and arranges its own storage access, so it already works under read-only. TTL background expiry, streams, and triggers already flow through `IsMainWriteable()`-gated paths, so they are covered automatically.

## Testing Decisions

Good tests here assert **external, observable behavior** — the coordinator setting's effect on write acceptance, serialization round-trips, and RPC version compatibility — not internal control flow. Tests should not couple to private reconciliation internals; those are exercised end-to-end.

**Unit tests:**
- **`CoordinatorClusterState` serialization/deserialization** (prior art: existing coordinator cluster-state ser/deser unit tests): round-trip the state including `global_read_only`, verify `DoAction` applies the delta, and verify `from_json` defaults `global_read_only` to `false` when the key is absent (backward compatibility).
- **RPC v2 versioning** for `PromoteToMainReq` and `UpdateDataInstanceConfig` (prior art: existing SLK RPC round-trip / downgrade tests, e.g. the `StateCheckRes`/`InstanceState` versioning tests): SLK save/load round-trip of the v2 payloads and `Upgrade`/`Downgrade` compatibility with v1.
- **SET/SHOW coordinator setting** (prior art: existing coordinator-setting query tests): the `global_read_only` setting is accepted, an invalid value is rejected, and `SHOW COORDINATOR SETTINGS` reports the value.
- **`ReplicationState` writing control**: `IsMainWriteable` / `EnableWritingOnMain` / `DisableWritingOnMain` transitions behave as expected for main and replica roles.

**E2E test (new file under `tests/e2e/high_availability/`, registered in that directory's `workloads.yaml` and `CMakeLists.txt`; prior art: `disable_writing_on_main_after_restart.py` for cluster setup and write-rejection assertions):**
- Build a cluster; after enabling read-only mode, write queries are rejected with the reworded error message.
- After disabling read-only mode, write queries succeed.
- `SHOW COORDINATOR SETTINGS` reflects the changed value.
- `CREATE SNAPSHOT` succeeds while the cluster is in read-only mode.

**Existing test to update:** `disable_writing_on_main_after_restart.py` asserts the write-rejection message prefix; update it to match the reworded `WriteQueryOnMainException` (keep a stable, assertable prefix).

## Out of Scope

- Per-database or per-tenant read-only mode — this feature is cluster-wide only.
- A separate reason/exception distinguishing "operator read-only" from "transient failover window" at the data instance (deliberately not introduced; single-bit design).
- Strict enforcement of read-only during a mixed-version rolling upgrade — behavior is best-effort and self-heals after upgrade completion.
- Standalone (non-HA) Memgraph — this is an enterprise/HA-only feature driven by the coordinator.
- Read-only enforcement changes to individual write sources (interpreter, TTL, streams, triggers) — these are already covered by the existing `IsMainWriteable()` gate.
- Freezing or pausing replication itself — replication continues normally; only new user/background writes on the main are blocked.

## Further Notes

- The enforcement points already exist in the interpreter (`IsMainWriteable()` gate) and TTL user-check; the interpreter side is considered complete and requires only the message reword.
- `EnableWritingOnMainRpc` is not currently sent from the coordinator reconciliation loop; writing is enabled today only at promotion time. This feature makes the reconciliation loop actively manage the main's writing state (both enabling via the gated promote path and disabling via `UpdateDataInstanceConfig`).
- The single-bit design means the coordinator's persisted `global_read_only` is the durable source of truth; a data instance's `writing_enabled_` is transient and reconstructed on restart, then reconciled to match the coordinator's intent.
