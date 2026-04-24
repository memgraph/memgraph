# DB Memory Review Plan

This plan turns the original review, the resolved work, and the follow-up comments into an execution checklist.

Use this as the reference for the remaining DB memory work on this branch.

Current state:

1. The core audit is finished.
2. The arena lifecycle model is verified as a reusable free-list model.
3. `ArenaAwareAllocator` has been retired in favor of the stateless `DbAwareAllocator` model.
4. The remaining open work is mostly cleanup, test alignment, and keeping the documented invariants current.

## Goals

1. Finish the arena-pool migration in a way that is explicit about ownership, fallback, and destruction.
2. Simplify allocator usage so `DbAwareAllocator` is the default transient DB-scoped allocator.
3. Audit all DB-facing execution paths so allocation and deallocation happen under the correct TLS or explicit arena ownership model.
4. Verify tenant-profile durability, privilege, replication, and query behavior against the final architecture.
5. Remove the old review table from the active branch narrative once the new plan is in place.

## What the original review missed or under-specified

The first review identified the right areas, but several topics were not nailed down tightly enough.

### 1. Arena lifecycle after database drop

The review tracked arena creation and reuse, but it did not settle the policy for what happens to a database arena pool when the database is dropped.

Open questions:

1. Are arenas destroyed immediately?
2. Are they kept in a reusable free list?
3. Is there a hybrid policy where the pool object dies but some arena resources are retained?

Why this matters:

1. It affects memory retention after DB churn.
2. It affects whether `CREATE DATABASE` can reuse prior arena resources.
3. It affects whether thread-id reuse can safely consult stale mappings.

Status:

1. Arenas are kept in a reusable free list.
2. DB drop returns arena indices to the pool instead of destroying the underlying jemalloc arenas.
3. This matches the requested reuse policy.

### 2. Failure policy for arena creation and hook installation

The review recognized failure cases, but not the full decision tree.

Open questions:

1. What happens if the first arena for a database cannot be created?
2. What happens if later per-thread arena acquisition fails?
3. What happens if hook installation fails after arena creation but before publication?

Why this matters:

1. Database creation must fail hard if the first arena cannot be prepared.
2. Later failures need a safe fallback, not a partial initialization leak.
3. Hook publication must not expose a half-initialized arena to later users.

Status:

1. The first arena remains a mandatory bootstrap step.
2. Later arena acquisition or hook-install failure falls back to the DB base arena.
3. The audit did not find a remaining violation of that policy.

### 3. Scope guard semantics were not fully specified

The original review asked for better placement of `DbArenaScope`, but the intended nesting and lifetime model was still fuzzy.

Open questions:

1. Should the scope support arbitrary nesting?
2. Should it only handle same-pool TLS nesting?
3. Should it represent a pool identity or a single concrete arena?

Why this matters:

1. Overly permissive nesting hides bugs.
2. Overly strict nesting breaks legitimate higher-level wrapper scopes.
3. Confusing pool identity with a specific arena leads to incorrect assumptions in worker code.

Status:

1. `DbArenaScope` is constrained to intentional same-pool TLS nesting.
2. It is not meant to be a generic arbitrary-arena stack.
3. The pool identity is kept distinct from a single concrete arena.

### 4. Allocator strategy was still transitional

The original review still contained both stateful and stateless allocator paths.

Open questions:

1. Can `ArenaAwareAllocator` be removed entirely?
2. Which code genuinely needs explicit arena capture?
3. Which code should be collapsed onto `DbAwareAllocator`?

Why this matters:

1. Two allocator models increase maintenance cost and confusion.
2. The final design should distinguish transient TLS-scoped work from long-lived ownership.

Status:

1. `ArenaAwareAllocator` has been retired.
2. `DbAwareAllocator` is now the default transient DB-scoped allocator.
3. Explicit arena capture remains only for the cases that genuinely need it.

### 5. Scope placement across the full DB execution pipeline was not exhaustively proven

The original review named many subsystems, but the coverage matrix was not complete enough to prove the scope model end to end.

Open questions:

1. Where exactly does DB selection become known in bolt / v2 / session / interpreter flow?
2. Which code paths allocate during `Pull`, `Commit`, `Abort`, GC, snapshot, stream startup, trigger initialization, or recovery?
3. Are cleanup paths using the correct arena context when work crosses threads?

Why this matters:

1. The code must not rely on ambient TLS without establishing it first.
2. The exact place where scope begins and ends matters more than the broad subsystem label.

Status:

1. The major DB-facing entry points were audited.
2. Query, stream, trigger, worker, recovery, and background paths now follow the DB pool model.
3. The one real issue found during the audit was the arena-0 restore bug in `CrossThreadMemoryTracking`, and it has been fixed.

### 6. Debug checks needed a more precise target model

The review added checks, but the expected abstraction was not fully normalized.

Open questions:

1. Should a check validate a pool or a concrete arena?
2. When is current TLS a meaningful assertion and when is it a false positive?
3. Which checks belong on transient allocators versus explicit arena owners?

Why this matters:

1. Cross-thread frees are legitimate in several paths.
2. A check that is too strict will fail valid cleanup.
3. A check that is too loose will miss real ownership mismatches.

Status:

1. The debug checks now validate the captured ownership model more precisely.
2. Explicit arena-owning deallocation paths check the arena they actually captured.
3. The checks are aligned with the pool-versus-arena split.

### 7. Tests were audited, but not yet tied cleanly to the final model

The review table lists tests, but the test plan still needs to map each test to the final ownership decision it protects.

Open questions:

1. Which tests should be updated because the allocator model changed?
2. Which tests are missing a fallback-path regression?
3. Which tests still encode transitional assumptions?

Why this matters:

1. The tests should guard the final architecture, not the interim one.
2. The branch needs regression coverage for the exact failure cases being normalized now.

Status:

1. The `arena 0` restore case now has a regression test.
2. A final review of test wording still makes sense, but the architectural risk is already covered.
3. Remaining work here is cleanup and alignment rather than discovery.

## Workstreams

The remaining work is now concentrated in a small number of cleanup and alignment tracks.
Workstreams A through E are now verified; workstream F is the main remaining cleanup lane.

## Execution Checklist

| Task | Owner | Priority | Status | Notes |
|---|---|---|---|---|
| Verify DB-drop arena lifecycle | Memory | P0 | Verified | Current code already returns arena indices to the reusable pool. `DbArena::~DbArena()` destroys no jemalloc arena; it restores hooks, purges, and lets `ArenaHandle` release the index back to `ArenaPool`. |
| Keep arena reuse through free list | Memory | P0 | Verified / maintain | `ArenaPool::Acquire()` reuses `pool_.back()` when available and only calls `arenas.create` on an empty pool. Do not replace this with arena destruction unless a new policy explicitly calls for it. |
| Confirm first arena creation must succeed | Memory + Storage | P0 | Verified | Database creation still fails hard if the base arena cannot be created or hooked. |
| Confirm later arena acquisition fallback | Memory + Storage | P0 | Verified | If per-thread arena creation or hook installation fails after the DB exists, the DB base arena is used instead. |
| Remove `ArenaAwareAllocator` | Memory | P0 | Done | Collapse transient DB-scoped allocations onto `DbAwareAllocator`. |
| Add allocator design comment | Memory | P1 | Done | `DbAwareAllocator` now notes that a stateful allocator is possible but intentionally not the default. |
| Audit `DbArenaScope` nesting model | Memory + Query | P0 | Verified | Scope manages same-pool TLS nesting and does not act like a generic arbitrary-arena stack. |
| Audit bolt and v2 entry points | Communication + Query | P0 | Verified | DB scope is installed only after the target DB is known and before DB-facing allocation can occur. |
| Audit `SessionHL` and `Interpreter` flow | Query | P0 | Verified | `Pull`, `Commit`, `Abort`, plan cache, and cleanup paths all execute under the right scope or explicit arena owner. |
| Audit GC, snapshot, and recovery paths | Storage | P0 | Verified | Periodic GC, forced GC, snapshot, WAL recovery, and helper threads use DB-aware scope or DB-aware thread setup. |
| Audit streams, triggers, and async workers | Storage + Query | P0 | Verified | Stream consumers, after-commit triggers, TTL, and async indexer threads resolve arenas from the DB pool correctly. |
| Audit `CrossThreadMemoryTracking` | Query | P1 | Done | The arena-0 restore case was fixed and tracking resolves the DB arena at execution time. |
| Re-check debug assertions | Memory | P1 | Done | The debug checks now validate the captured ownership model more precisely. |
| Verify tenant profile durability payload | DBMS | P1 | Verified | The durable KV record contains the profile name, memory limit, database list, and versioning needed for restore. |
| Reconfirm tenant privileges and replication | DBMS + Auth | P1 | Done / verify | Current state says tenant-profile mutations use `MULTI_DATABASE_EDIT`, not `STATS`, and enterprise/license gating is in place. Keep this checked against future refactors. |
| Align tests to final architecture | Tests | P1 | In progress | Update any tests that still encode transitional assumptions. Add a narrow fallback-path regression if missing. The dead post-destruction `ArenaRegistration` note has been removed; remaining work is mostly wording and scope cleanup. |
| Remove old tracker from active branch narrative | Docs / Cleanup | P2 | Done | `review_issue_table.md` has already been removed from git tracking locally; keep it only if you want the local copy for reference. |

### Verified arena reuse detail

The arena lifecycle question is now answered by the current code path, not just by policy:

1. `DbArena` acquires arenas from `ArenaPool`.
2. `ArenaHandle` returns indices to `ArenaPool::Release()` on destruction.
3. `ArenaPool::Release()` pushes the index into `pool_`.
4. `ArenaPool::Acquire()` reuses `pool_.back()` before calling `je_mallctl("arenas.create", ...)`.
5. `ArenaPool::Drain()` only clears the reusable list; it does not destroy the underlying jemalloc arena objects.

So the correct implementation behavior is already “free list and reuse,” which matches your preference. The remaining work is to preserve and document that policy, not replace it.

### Workstream A: Arena pool lifecycle

Objective: define and implement the DB-drop and arena-reuse policy.

Tasks:

1. Decide what happens to arena pools when a database is dropped.
2. Decide whether the dropped DB's arenas are destroyed, pooled, or retained in a controlled free list.
3. Make the database creation path require a successfully initialized first arena.
4. Define fallback behavior for later arena acquisition or hook installation failures.
5. Audit thread-id reuse assumptions under the chosen lifecycle model.

Expected output:

1. A single documented lifecycle policy.
2. A deterministic create/drop behavior.
3. Safe fallback for non-initial arena failures.

### Workstream B: Allocator simplification

Objective: converge transient DB-scoped code on `DbAwareAllocator`.

Tasks:

1. Remove `ArenaAwareAllocator` and its associated implementation, tests, and call sites.
2. Keep explicit arena-capturing helpers only for long-lived or cross-thread ownership cases.
3. Add a short comment above `DbAwareAllocator` noting that a stateful allocator is possible but intentionally not the default design.
4. Update any allocator-related documentation to match the final split.

Expected output:

1. One default allocator path for transient DB-scoped work.
2. One explicit ownership path where needed.
3. No ambiguous allocator naming left in the codebase.

### Workstream C: TLS scope audit

Objective: prove that every DB-facing allocation or deallocation occurs under the right scope or owner.

Tasks:

1. Audit communication/bolt and communication/v2 entry points.
2. Audit `SessionHL` and `Interpreter` transitions from DB selection to query work.
3. Audit `Pull`, `Commit`, `Abort`, and query cleanup.
4. Audit GC, forced `FREE MEMORY`, snapshot, recovery, streams, triggers, and replication paths.
5. Audit compressors, property buffers, vector index work, constraints, and any async maintenance code.
6. Audit `CrossThreadMemoryTracking` so it resolves the correct arena at execution time inside the worker lambda.
7. Audit stream and trigger initializers so they obtain the arena they need from the pool before entering the RAII scope.

Expected output:

1. A complete list of scope boundaries.
2. No DB-facing allocation path that depends on accidental TLS state.
3. No cleanup path that frees memory outside the intended ownership model.

### Workstream D: Debug checks and invariants

Objective: make the assertions useful without overconstraining legal behavior.

Tasks:

1. Revisit all debug assertions related to arena ownership.
2. Ensure `DbAwareAllocator` debug checks assert the right TLS precondition.
3. Ensure explicit arena-aware owners assert against the arena they actually captured.
4. Remove or relax checks that wrongly assume current TLS during cross-thread deallocation.

Expected output:

1. Assertions that catch bugs without blocking valid execution.
2. Checks aligned with the final pool-versus-arena semantics.

### Workstream E: Tenant profile verification

Objective: finish the durable tenant-profile story and confirm it matches the final system behavior.

Tasks:

1. Audit the durable KV payload for tenant profiles.
2. Confirm restart restore reconstructs all required state.
3. Verify tenant profile privilege requirements remain correct.
4. Recheck enterprise gating and license checks.
5. Reconfirm replication behavior for tenant profile mutations.

Expected output:

1. Tenant profile state is fully reconstructible from durable data.
2. The privilege and replication model are stable and documented.

### Workstream F: Test alignment and cleanup

Objective: make the tests match the final code paths and remove obsolete review artifacts.

Tasks:

1. Review the tests added during the DB memory work.
2. Update tests that encode now-obsolete transitional assumptions.
3. Add regressions for the most important fallback and scope cases if missing.
4. Remove `review_issue_table.md` from the active git history while keeping the local copy if desired.
5. Rewrite the remaining memory-tracking comments so they use the DB base-arena / per-thread arena language.
6. Keep intentionally unrelated auth privilege tests as-is, but make sure they are not cited as evidence for the memory migration.
7. Remove any executable dead-end notes that do not assert a behavior, such as post-destruction lifetime comments that only document undefined behavior.

Expected output:

1. A test suite aligned with the final architecture.
2. A clean branch narrative without duplicate review trackers.
3. No stale comment language that implies the retired allocator split or a constructor-owned arena model.

## Suggested order of execution

The work should be done in this order to minimize rework:

1. Arena pool lifecycle.
2. Allocator simplification.
3. TLS scope audit.
4. Debug checks and invariants.
5. Tenant profile verification.
6. Test alignment and cleanup.

Why this order works:

1. Lifecycle and allocator decisions influence the rest of the audit.
2. Scope placement depends on knowing which allocator model remains.
3. Debug checks should be validated after the ownership model is final.
4. Tests should be updated last so they encode the finished behavior.

## Acceptance criteria

The plan is complete when all of the following are true:

1. DB drop behavior for arena pools is explicitly defined and implemented.
2. First-arena creation is mandatory during DB creation, and later arena failures degrade safely.
3. `ArenaAwareAllocator` is removed or fully retired from production use.
4. `DbArenaScope` and related TLS handling are audited across all DB-facing execution paths.
5. `CrossThreadMemoryTracking` and initializer-based workers are using the right pool-to-arena resolution model.
6. Debug checks validate the final ownership model without false positives on legitimate cross-thread cleanup.
7. Tenant profile durability, privileges, and replication are confirmed against the final architecture.
8. The tests reflect the final design, not the transition state.
9. `review_issue_table.md` is no longer part of the tracked branch history.

## Notes to keep in mind while implementing

1. Prefer explicit pool ownership over implicit assumptions.
2. Prefer TLS scopes only where the current thread is actually the right execution context.
3. Prefer captured arena ownership where lifetime or cross-thread behavior makes TLS insufficient.
4. Keep comments focused on invariants and flow, not on historical implementation details.
5. Avoid reintroducing direct jemalloc-specific logic at call sites when a DB-aware abstraction can hide it.
6. Treat any `arena_idx` field carefully and verify whether it means a base arena, a pool identifier, or a concrete acquired arena.

## Short version

Finish the arena lifecycle policy, remove the transitional allocator split, audit every DB-facing execution path for correct TLS or explicit ownership, then lock the behavior in with updated tests and cleanup.
