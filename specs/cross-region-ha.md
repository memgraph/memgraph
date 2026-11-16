# PRD: Cross-region high availability

Status: Draft

Summary: group HA data instances into regions, designate one primary region, and let the coordinator keep the main and
all synchronous replication inside it while replicating asynchronously to the other regions.

## Problem Statement

A Memgraph HA cluster today has no notion of where its data instances live. Operators who spread instances across
regions must pick a replication mode per instance by hand, failover may promote an instance in a remote region, and
SYNC/STRICT_SYNC replication to a remote region puts cross-region latency on every commit. There is no way to say
"this region is where the main lives; everything else is a disaster-recovery standby", and no single lever to move the
cluster to another region when a region is lost.

## Solution

Regions become first-class objects in the coordinator's Raft state. Operators add regions, assign data instances to
them, and designate a primary region. Once a primary region is set, the cluster runs in cross-region mode:

- The coordinator derives each replica's replication mode from its region: replicas inside the primary region use the
  cluster-wide `region_replication_mode` (SYNC or STRICT_SYNC), replicas in other regions are always ASYNC.
- The main always lives in the primary region. The coordinator's reconciliation loop converges the cluster towards
  that state: it bootstraps a main if there is none, fails over into the primary region if the main is elsewhere, and
  re-applies replica modes whenever they drift.
- `SET '<region>' AS PRIMARY REGION` only writes the desired state to Raft. It is both the switch that turns a
  single-region cluster into a cross-region one and the manual disaster-recovery lever for moving to another region.

## User Stories

1. As an operator, I want to define the regions of my cluster explicitly, so that a misspelled region name is rejected
   instead of silently creating a new region.
2. As an operator, I want to register a data instance into a region, so that the coordinator knows where it lives.
3. As an operator, I want to fix or change an instance's region with UPDATE CONFIG, so that a misconfiguration does
   not force me to unregister and re-register the instance.
4. As an operator of an existing single-region cluster, I want to label my running instances with regions and then
   switch to cross-region mode without downtime, so that I can grow into a second region.
5. As an operator starting a new cross-region cluster, I want `SET '<region>' AS PRIMARY REGION` to be enough to get a
   main, so that I do not have to pick one by hand.
6. As an operator, I want replication within the primary region to be synchronous and replication to other regions to
   be asynchronous, so that commits never wait on cross-region latency.
7. As an operator, I want to choose between SYNC and STRICT_SYNC for in-region replication with one coordinator
   setting, so that the whole cluster uses one consistent mode.
8. As an operator, I want failover to only ever promote an instance in the primary region, so that the cluster never
   moves to another region without my decision.
9. As an operator losing a whole region, I want to move the cluster to another region with one query, accepting
   possible loss of asynchronously replicated data, so that I can recover service.
10. As an operator, I want `SHOW REGION INFO` to list the regions, their instances, and which one is primary, so that I
    can verify the topology.
11. As an operator, I want to shrink back from two regions to one, so that I can decommission a region.
12. As an application developer, I want reads to be served only from the primary region when the operator asks for
    it, so that my reads avoid cross-region lag where possible.
13. As an operator, I want `SHOW INSTANCE` and `SHOW INSTANCES` to keep their current output, so that my existing
    tooling keeps working.
14. As an operator doing a rolling upgrade, I want existing clusters to behave exactly as before until I set a primary
    region, so that upgrading is safe.

## User-facing surface

### Queries (coordinator only)

| Query | Behavior |
|---|---|
| `ADD REGION 'eu-west'` | Adds a region. Error `REGION_EXISTS` if it already exists. |
| `DROP REGION 'eu-west'` | Drops a region. Rejected if the region does not exist (`NO_SUCH_REGION`), still has instances, or is the primary region. |
| `SET 'eu-west' AS PRIMARY REGION` | Sets the primary region. The region must exist and contain at least one instance. Setting the current primary region again is a no-op success. Only writes to Raft; the reconciliation loop does the rest. |
| `SHOW REGION INFO` | One row per region, sorted by name: `region` (string), `instances` (sorted list of instance names, may be empty), `is_primary_region` (bool). Forwarded to the leader. |
| `REGISTER INSTANCE i1 WITH CONFIG {..., "region": "eu-west"}` | Assigns the instance to a region. The region must exist (`NO_SUCH_REGION`). |
| `UPDATE CONFIG FOR INSTANCE i1 {"region": "us-east"}` | Changes an instance's region. `bolt_server` and `region` are both optional; at least one is required. `region` is rejected for coordinators. |

Region names are string literals, case-sensitive, non-empty. All write queries go through the Raft leader and use the
existing coordinator privilege.

### Coordinator settings (Raft-replicated, `SET COORDINATOR SETTING`)

| Setting | Default | Meaning |
|---|---|---|
| `region_replication_mode` | `SYNC` | Replication mode for replicas inside the primary region. Accepts `SYNC` or `STRICT_SYNC`. Changeable at runtime; the loop re-applies modes. |
| `prefer_reading_from_primary_region` | `false` | When true, the routing table lists as readers only the replicas in the primary region (plus the main if `enabled_reads_on_main` is set). If that set is empty it falls back to all replicas. No effect while no primary region is set. |

### Unchanged

`SHOW INSTANCE` and `SHOW INSTANCES` output is unchanged.

## Cluster lifecycle

### Before a primary region is set

The cluster behaves exactly as HA does today. Regions can be added and dropped; instances may carry a region or not,
and regions are only labels. Instances register with an explicit replication mode (`AS ASYNC`, `AS STRICT_SYNC`, or the
SYNC default), as today. UPDATE CONFIG can add, change, or remove (`"region": null`) an instance's region.
`SET INSTANCE ... TO MAIN`, failover, and all other HA behavior are unchanged.

### The switch: the first `SET ... AS PRIMARY REGION`

- Every registered instance must have a region, otherwise the query fails with `INSTANCES_WITHOUT_REGION_EXIST`.
- The target region must contain at least one instance.
- In a single Raft entry the coordinator sets `primary_region` and clears every instance's stored replication mode.
- No RPCs are sent by the query itself. The reconciliation loop converges the cluster from the next cycle on.
- There is no way back to a cluster without a primary region.

### After the switch (cross-region mode)

- REGISTER INSTANCE requires a region and rejects `AS ASYNC` / `AS STRICT_SYNC`.
- `SET INSTANCE ... TO MAIN` is rejected.
- Unregistering the last instance of the primary region is rejected.
- Moving the last instance out of the primary region with UPDATE CONFIG is rejected.
- Removing an instance's region is rejected.
- Further `SET ... AS PRIMARY REGION` calls only write to Raft.

### Shrinking to a single region

1. `SET 'eu' AS PRIMARY REGION`
2. Unregister every instance in `us`.
3. `DROP REGION 'us'`

The result is a cross-region cluster with one region, which behaves like a single-region cluster with every replica in
`region_replication_mode`.

## Desired state the reconciliation loop converges to

Once a primary region is set, the leader's reconciliation loop (the instance success/fail callbacks) enforces:

1. **Resolved replica modes.** A replica in the primary region uses `region_replication_mode`; any other replica uses
   ASYNC. The coordinator always sends concrete, resolved modes to data instances.
2. **Bootstrap.** If there is no current main, the most up-to-date alive instance in the primary region is promoted.
3. **Main placement.** If the main is outside the primary region, the loop performs an unplanned failover restricted to
   primary-region candidates. Data loss within the configured lag is accepted. `max_failover_replica_lag` is respected:
   if no primary-region candidate passes it, the current main stays in place and the loop retries. The old main is
   fenced by the existing mechanism (new main UUID in Raft, promotion, demotion of the old main on its next ping;
   replicas reject the old UUID).
4. **Crash failover.** Candidates are only primary-region instances. There is no automatic failover into another
   region; if no primary-region instance can be promoted the cluster has no writable main until one recovers or the
   operator sets another primary region. `sync_failover_only` is ignored in cross-region mode.
5. **Mode convergence.** The main reports the mode of each registered replica in its `StateCheckRpc` response. If any
   reported mode differs from the resolved mode, the loop re-sends `PromoteToMainRpc` to the current main with the
   same main UUID and the full resolved replica list. Re-promotion tears down all replica clients under the
   replication-state lock and keeps writing disabled until every replica is re-registered, so SYNC and STRICT_SYNC
   replicas never coexist and no commit observes a partial mode change. A replica that is simply missing on the main
   is still added with `RegisterReplicaOnMainRpc` using its resolved mode.

## Implementation Decisions

**`DataInstanceContext`.** Gains `std::optional<std::string> region`. Serialized to JSON only when set; a missing key
reads back as `nullopt`.

**`ReplicationClientInfo::replication_mode`.** Becomes `std::optional<ReplicationMode>` everywhere: Raft JSON and the
wire. Stored as `nullopt` for every instance after the switch. The registration-time SYNC/STRICT_SYNC mixing check does
not apply to instances without a stored mode.

**RPC versioning.** `DemoteMainToReplicaRpc`, `RegisterReplicaOnMainRpc` and `PromoteToMainRpc` get new versions
carrying the optional mode, with `Upgrade`/`Downgrade`. `Downgrade()` maps `nullopt` to ASYNC. On a data instance,
receiving `nullopt` in the register-replica path (`DoRegisterReplica`) is a hard failure: log and fail the RPC.
`StateCheckRpc`'s `InstanceState` gets a new version reporting each registered replica's mode. `UpdateConfigRpc`
(coordinator-to-coordinator forward) gets a new version for the optional bolt endpoint and optional region.

**`CoordinatorClusterState` / delta.** Gains the set of regions, `std::optional<std::string> primary_region`,
`region_replication_mode` and `prefer_reading_from_primary_region`, threaded through the delta, `DoAction`,
equality, JSON, the state-machine serialization, and the Raft-state getters. Defaults when absent from older logs:
no regions, no primary region, SYNC, false.

**Atomic Raft entries.** The switch (set primary region + clear all modes) is a single Raft entry. Unregistering and
region changes are validated against the primary-region invariants on the leader before appending.

**Routing.** `CreateRoutingTable` filters readers to the primary region when `prefer_reading_from_primary_region` is
true and a primary region is set, falling back to all replicas when the filtered set is empty.

**ISSU.** Coordinators are upgraded before data instances. Old coordinators are not expected to replay log entries
written by new ones.

## Testing Decisions

- Unit: cluster-state JSON round-trip with and without the new fields (including logs written before this feature);
  mode resolution; RPC `Upgrade`/`Downgrade` including `nullopt` → ASYNC.
- Coordinator: query validation errors (`NO_SUCH_REGION`, `REGION_EXISTS`, `INSTANCES_WITHOUT_REGION_EXIST`, dropping
  primary/non-empty region, last-instance rules, REGISTER with region + explicit mode after the switch,
  SET INSTANCE TO MAIN after the switch).
- E2E HA: upgrade path (label running cluster, switch, modes converge without failover when the main is already in
  the primary region); fresh bootstrap via SET PRIMARY REGION; placement failover after changing the primary region;
  crash failover stays inside the primary region; no failover when the primary region has no alive instance;
  `region_replication_mode` change SYNC → STRICT_SYNC converges; `prefer_reading_from_primary_region` routing and
  fallback; `SHOW REGION INFO` output.
- New coordinator settings must be registered in the e2e configuration checks.

## Out of Scope

- Planned (lossless) failover between regions.
- Client-region-aware read routing (reads preferring the client's own region).
- Converting a cross-region cluster back to one without a primary region.
- Automatic failover across regions.
