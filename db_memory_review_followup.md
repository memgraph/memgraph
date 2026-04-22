# DB Memory Review Follow-Up

This note consolidates the original review issues, the work that has already landed, the new architectural questions you added afterward, and the next steps that would tighten the implementation further.

It is meant to replace the old working table as the active narrative for the branch while keeping the detailed history in `review_issue_table.md` locally.

## What the initial review was really about

The original review table was not just a list of fixes. It was tracking a broader migration from a single database arena model to a database arena pool model, plus the fallout from that change across allocation, threading, query execution, durability, and tenant metadata.

The recurring themes were:

1. Arena ownership had to become explicit and correct under thread reuse, DB drop, and hook installation failure.
2. DB-aware work needed a clean TLS-based scope model, but long-lived owners still needed explicit arena capture where lifetime outlives scope.
3. The allocator story needed to be simplified around one transient TLS allocator and a smaller number of explicit ownership helpers.
4. Query, storage, stream, trigger, GC, snapshot, replication, and recovery paths all needed coverage so no DB-facing code silently allocated outside the right arena context.
5. Tenant profile durability, privileges, and replication behavior needed to be verified separately because that system is durable DBMS state rather than normal per-query state.

## What has already been resolved

The review table shows a lot of the high-risk work is already done. The most important completed items are:

1. `storage::Config` no longer carries runtime arena registration or DB embedding tracker state. That runtime state now comes from `Database`/`Storage` construction.
2. DB arena creation and hook installation were hardened so failures do not publish partially initialized arenas.
3. `DbArenaScope` was made into a proper RAII TLS saver/restorer, and the non-jemalloc implementation was added.
4. Long-lived DB-owned threads were audited and wired to acquire DB arenas correctly, including GC, after-commit trigger workers, TTL, async indexing, and stream consumers.
5. Query commit, abort, pull execution, and cross-thread memory tracking were audited and fixed where needed.
6. The allocator and debug-check story was clarified: `DbAwareAllocator` is the stateless TLS allocator, while explicit arena-capturing helpers remain for long-lived ownership.
7. Vector, compressor, property buffer, WAL, delta container, durability recovery, and tenant profile flows were all reviewed and brought into the same memory-accounting model.

If we read the table as a migration log, the branch has already crossed the main safety threshold: the dangerous gaps were mostly around ownership semantics, not around isolated call sites.

## What your new notes are really asking

Your follow-up comments narrow the remaining work into a few concrete questions.

### 1. What happens to arenas on database drop?

This is the biggest open lifecycle question.

The old model had one arena per database and could keep a reusable free list across DB drop/create cycles. The new model has an arena pool per database, with lazy creation of arenas per thread pair.

The unresolved point is whether dropping a database should:

1. destroy its arena pool immediately,
2. move the pool to a reusable free list,
3. or keep some subset of arenas around for reuse while invalidating the DB association.

That choice affects memory retention, thread-id reuse safety, and whether a future `CREATE DATABASE` can cheaply reuse previous infrastructure.

### 2. What happens when arena acquisition or hook installation fails?

You already defined the intended behavior:

1. The first arena for a database must be created during database creation and must succeed.
2. If any later per-thread arena acquisition or hook installation fails, the system should fall back to the DB’s first arena for that database.

That fallback policy is sensible, but it needs a careful audit because the code has to distinguish:

1. failure to create a new arena,
2. failure to install hooks,
3. failure while publishing the arena into the pool,
4. and failure on DB creation itself, where there is no safe fallback.

### 3. Should `DbArenaScope` cover only same-TLS pool nesting?

Yes, based on your note, the scope should be narrower than a general “everything in DB memory” guard.

The intended model appears to be:

1. `DbArenaScope` manages TLS for a known DB arena pool context.
2. It can nest only within the same TLS arena pool family.
3. It should not become a generic arbitrary-arena stack because that hides ownership bugs.

That means the scope should protect the current DB-aware execution context, not act like a universal per-arena stack machine.

### 4. Should TLS scope and DB-aware threads point to the pool, not a single arena?

This is an important design refinement.

The current direction seems to be:

1. the database owns an arena pool,
2. the TLS scope points at that pool identity,
3. and a thread may resolve or acquire a concrete arena for the duration of the RAII guard.

That is different from storing one concrete arena everywhere and expecting it to represent the whole DB.

### 5. Should `DbAwareAllocator` be the only allocator family?

Your note is clear: yes.

The stateful `ArenaAwareAllocator` should be removed, and the codebase should converge on `DbAwareAllocator` as the default allocator for DB-scoped transient work.

The remaining explicit arena-capturing logic should stay only where ownership is genuinely long-lived or cross-thread by design.

### 6. Are the debug checks pointing at the correct abstraction?

The current debug checks should validate against the DB arena pool ownership model, not against a specific arena index unless the code truly captured a single arena.

That matters because some flows:

1. use one arena for the lifetime of a scope,
2. some use a specific acquired arena,
3. and some only need to know that they are executing under the correct DB pool.

The checks should mirror that split instead of overfitting to one narrow case.

### 7. Are all DB-facing execution flows covered by the scope guard?

This is still the right thing to audit.

The major paths you called out are:

1. communication/bolt,
2. communication/v2,
3. `SessionHL`,
4. `Interpreter`,
5. `Pull`,
6. background GC,
7. snapshot and recovery,
8. triggers,
9. streams,
10. compressor/property buffer cleanup,
11. replication,
12. and indices/constraints.

The branch should be checked for places where the code assumes “the thread already knows the DB” without proving that the TLS scope is installed first.

## Current risk assessment

My read is that the branch is in a better state than the raw notes suggest, but there are still a few real risks:

1. Arena-drop behavior is a lifecycle policy gap, not just an implementation detail.
2. The allocator migration to `DbAwareAllocator` only is still incomplete if any `ArenaAwareAllocator` use remains.
3. Query execution and background workers still need a final scope-placement audit to ensure TLS is established at the exact point allocations may happen.
4. Cross-thread tracking and thread-initializer-based workers need to be audited specifically for “pool first, concrete arena later” behavior.
5. The tenant-profile model is mostly verified, but its durable KV payload should still be checked against the branch’s final data model.
6. The current review tracker is no longer the best place to keep active work; it should be retired once the follow-up items are captured elsewhere.

## Recommended next steps

### P0: finish the arena-pool lifecycle policy

1. Decide and document the DB-drop behavior for arena pools.
2. Define the exact fallback behavior for arena acquisition and hook installation failures after DB creation.
3. Make the first arena creation path a hard requirement in database creation and keep later failures degradable.
4. Confirm whether thread-id reuse still preserves correctness under the new pool model.

Acceptance criteria:

1. The drop path has one explicit policy.
2. The create path fails cleanly when the first arena cannot be established.
3. Later arena failures do not break existing DB execution and fall back to the base arena as intended.

### P0: finish the allocator simplification

1. Remove `ArenaAwareAllocator` and any associated tests, call sites, and comments.
2. Standardize transient DB-scoped allocations on `DbAwareAllocator`.
3. Keep explicit arena-capturing helpers only where ownership is long-lived or cross-thread.
4. Add the short comment above `DbAwareAllocator` noting that a stateful allocator is possible but intentionally not the default design.

Acceptance criteria:

1. No production code relies on the removed allocator family.
2. Tests and comments match the new single-default allocator model.
3. Ownership-sensitive code still has a deliberate explicit-arena path where needed.

### P0: audit the TLS scope placement end to end

1. Walk the communication, session, interpreter, and query execution flow from DB selection through `Pull`, `Commit`, `Abort`, and cleanup.
2. Verify every storage/DB allocation and deallocation happens under a valid `DbArenaScope` or an explicit arena-capturing owner.
3. Audit `CrossThreadMemoryTracking` so it captures the DB pool and resolves the concrete arena at execution time inside the worker lambda.
4. Audit stream and trigger initializers so they obtain a separate arena from the pool and then install the RAII scope correctly.

Acceptance criteria:

1. Every DB-facing entry point has a clear scope boundary.
2. Cross-thread workers do not depend on stale TLS state from the parent thread.
3. Initializer-based workers own their arena setup explicitly and predictably.

### P1: re-check debug assertions and ownership checks

1. Verify debug checks match the pool-versus-arena abstraction at each call site.
2. Ensure deallocation checks do not incorrectly require current TLS when cross-thread frees are valid.
3. Keep the checks focused on the cases where the allocator or owner actually has enough information to validate.

Acceptance criteria:

1. Debug checks catch real mismatches without producing false positives on legitimate cross-thread cleanup.
2. Pool-aware code validates pool ownership, not incidental thread state.

### P1: finish the remaining subsystem audit

1. Review compression and property buffer cleanup.
2. Reconfirm durability recovery, snapshot, WAL recovery, and forced GC.
3. Recheck indices, constraints, vector indexes, and any async update paths.
4. Confirm replication and storage client paths still use the intended DB-aware ownership model.

Acceptance criteria:

1. No DB-facing cleanup path is left outside the intended scope model.
2. The audit result is documented in a way that survives future refactors.

### P1: verify tenant profile durability and privileges one last time

1. Confirm the durable KV payload contains everything required to reconstruct tenant-profile state.
2. Verify the privilege choice remains correct for future query-routing changes.
3. Keep the enterprise/license checks and replication behavior consistent with the final query model.

Acceptance criteria:

1. Startup restore still reconstructs the same tenant-profile state after restart.
2. The privilege model remains aligned with DBMS mutation semantics.

### P2: update the tests and retire the old review table

1. Review the added tests against the final architecture and prune anything that now encodes the wrong assumption.
2. Add a narrow regression where a DB-aware worker falls back to the base arena after a controlled acquisition failure, if that behavior is intended to remain.
3. Remove `review_issue_table.md` from git while keeping your local copy if you still want it as reference.

Acceptance criteria:

1. Tests describe the final ownership model, not the transitional one.
2. The branch no longer carries two active review narratives.

## Bottom line

The branch has already resolved most of the dangerous implementation work. The remaining job is not “keep fixing random allocations” so much as “finish the policy and simplify the model.”

If I had to compress the next phase into one sentence, it would be:

make the arena pool lifecycle explicit, collapse allocator usage to the stateless TLS path, and verify every remaining DB-facing thread or cleanup path resolves the correct arena at the moment it actually allocates or frees.
