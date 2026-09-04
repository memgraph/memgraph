# U3 — main_lock BEGIN park (design spec)

Branch `feat/adaptive-commit-lock-scheduling` off #4685. Gated on flag `lockfree-read-snapshot`
(same flag as commit-park; when OFF every path below is byte-identical to base).

## Goal
Today BEGIN blocks-with-timeout on `main_lock_` inside `CurrentDB::SetupDatabaseTransaction`
(interpreter.cpp:196) via `db_acc->Access/UniqueAccess/ReadOnlyAccess(..., timeout)`. A blocked pool
worker occupies a thread while a DDL/UNIQUE/READ_ONLY holder drains. U3 replaces the block with a
bounded-try + PARK-the-pool-worker driver, mirroring the commit-park (U4) exactly but for the
BEGIN/main_lock path. This ports the S2e "main lock wins" onto the #4685 base.

Difference vs engine_lock (which we deliberately do NOT park): main_lock is the COLD path
(DDL/schema/UNIQUE/READ_ONLY), not the read hot path, so parking here cannot reproduce the
RW-fast/UNIQ_RO/UNIQ_UQ pure-contention regressions that killed engine-lock parking.

## The four modes and what U3 does per mode
| BEGIN mode | main_lock hold | gates anyone? | park style |
|---|---|---|---|
| READ  | shared READ  | no | STATELESS retry (no PendingScope) |
| WRITE | shared WRITE | no | STATELESS retry (no PendingScope) |
| READ_ONLY | shared READ_ONLY | yes (ro_pending gates WRITE) | HOLD `ReadOnlyPendingScope` across park |
| UNIQUE | exclusive | yes (unique_pending gates all shared) | HOLD `UniquePendingScope` across park |

READ/WRITE register no pending state (they gate nobody), so their park is a plain re-probe
(`try_acquire`) on wake — identical shape to commit-park. UNIQUE/READ_ONLY must register-pending
ONCE up front and hold it for the whole park so writer-preference priority is preserved while the
worker is off-thread; each wake calls `scope.try_acquire()`.

## VERIFIER-MANDATED FIXES (logic-verifier 2026-09-03, folded — MUST implement)
- **Q1c (HIGH) — no PendingScope leak on abandonment.** The scope variant lives in the session's
  pending-begin state. The session is NOT destroyed on a timeout/error transition (client can RESET
  and continue), so a scope left live keeps `unique_pending_count`/`ro_pending_count` elevated →
  process-wide admission block on that ResourceLock until disconnect. FIX: in `FinishPendingBegin_`,
  place an `utils::OnScopeExit` at entry that resets the scope to `std::monostate`
  (`pending_begin_scope_.emplace<std::monostate>()`), and CANCEL it only on the Reschedule return
  (the one path that must keep the scope alive for the next wake). Timeout, ClientError, and Done all
  fall through to the reset. Do NOT rely on the outer session destructor.
- **Q4b (RISK) — finite MainLock deadline.** `ParkAdmission` for `WaitResource::MainLock` MUST pass
  the stashed finite deadline (`now + GetStorageAccessTimeoutSec()` captured at the throw site), NOT
  the commit-park's `time_point::max()`. Copy-pasting the commit-park call silently drops the BEGIN
  timeout contract. Add `DMG_ASSERT(deadline != std::chrono::steady_clock::time_point::max())` on the
  MainLock park path.

## Novel correctness surface (verify this)
A `PendingScope` that is registered but never deregistered blocks its mode process-wide (see
resource_lock.hpp:91-93, 625-643). So the PendingScope MUST be destroyed on every exit path of the
park: success (ownership transfers out via `try_acquire()` → guard), timeout (throw), client
disconnect / session teardown, and pool drain/shutdown. It lives inside the bolt session's
pending-begin state; the state object's destructor must run on all those paths.

## Components

### U3a — ResourceLock notify callback + non-blocking BEGIN probe
1. `src/utils/resource_lock.hpp`: add a single optional hook
   `std::move_only_function<void()> on_notify_all_;` (needs `<functional>`), with a setter
   `void SetNotifyHook(std::move_only_function<void()>)`. Fire it inside `maybe_notify` AFTER
   `lock.unlock()`, ONLY when `kind == NotifyKind::All`:
   ```cpp
   void maybe_notify(std::unique_lock<std::mutex> &lock, NotifyKind kind) {
     lock.unlock();
     if (kind == NotifyKind::All) {
       cv.notify_all();
       if (on_notify_all_) on_notify_all_();   // fired off-mtx: covers ALL six NotifyKind::All points
     }
   }
   ```
   Rationale (finalized design decision #1): the six NotifyKind::All points (4 release +
   downgrade_to_read + 2 pending-gate clears in unregister_pending) all funnel through maybe_notify,
   so one hook covers them with no call-site enumeration. Fires off the internal mtx → the hook may
   take the pool's parked_mtx_ without inverting lock order (decision #5). Other ResourceLock
   instances leave the hook empty → one null-check of overhead, no behavior change.

2. Storage side: a non-blocking BEGIN accessor acquisition. `AcquireGuardOrThrow` (storage.hpp:565)
   currently blocks-with-timeout. Add a non-blocking sibling that the query layer can call to decide
   whether to park, WITHOUT throwing on miss:
   - For READ/WRITE: `main_lock_.try_lock_shared<mode>()` → on success adopt into a guard; on miss
     return "would block".
   - For UNIQUE/READ_ONLY: this is the PendingScope path; the probe is `scope.try_acquire()`, so the
     scope (not the storage helper) owns the retry. The storage helper only needs to expose enough
     to build the Accessor from an already-acquired guard (the Accessor ctor already takes a
     `ResourceLockGuard`), plus access to `main_lock_` so the driver can construct the right
     PendingScope. Prefer: expose `main_lock_` acquisition through a small storage method rather than
     leaking the lock — e.g. `Database::TryBeginAccess(acc_type, iso)` returning
     `std::optional<unique_ptr<Accessor>>` for READ/WRITE, and a
     `Database::MakePendingBeginScope(acc_type)` returning a type-erased pending scope for
     UNIQUE/READ_ONLY. FINAL API SHAPE TBD in implementation — keep OFF-path byte-identical.

3. Wire the hook: when flag ON, storage sets `main_lock_.SetNotifyHook([pool]{ pool->WakeMatching({WaitResource::MainLock}); })`
   once at storage construction (or when the pool becomes known). Must be torn down before the pool
   dies (RC-3 shutdown order): clear the hook in storage teardown / before pool destruction so a late
   notify never calls into a dead pool.

4. `src/query/exceptions.hpp`: `class BeginWouldBlockException final : public std::exception`
   (NOT BasicException — must not be caught by the interpreter's generic catch(BasicException),
   exactly like CommitWouldBlockException at :626).

5. `CurrentDB::SetupDatabaseTransaction` (interpreter.cpp:196): when flag ON, replace the
   blocking-with-timeout acquire with: bounded try; on miss throw `BeginWouldBlockException`
   carrying (acc_type, remaining deadline). The finite BEGIN timeout is preserved
   (`GetStorageAccessTimeoutSec` — user: "the begin timeout is safe, keep it"): the driver parks
   only until that deadline, then throws the SAME timeout exception the blocking path throws today.

### U3b — bolt PendingBegin park/resume driver (mirror PendingCommit U4b/U4c)
- `state.hpp`: add `State::PendingBegin` + reuse/extend the outcome enum.
- `session.hpp`: `HasPendingBegin`, `StashPendingBegin(...)`, `FinishPendingBegin_`, members holding
  the pending-begin state INCLUDING the `std::variant<monostate, UniquePendingScope,
  ReadOnlyPendingScope>` scope + acc_type + deadline + the stashed message/qid needed to re-drive
  the query after the accessor is acquired.
- `handlers.hpp`: catch `BeginWouldBlockException` where a BEGIN/first-query is prepared, BEFORE the
  generic catch; stash + return `State::PendingBegin`.
- `v2/session.hpp`: `DoWork` HasPendingBegin branch → park via
  `ParkAdmission(cont, id, deadline, {WaitResource::MainLock, mode})` (FINITE deadline, unlike
  commit). On wake: `scope.try_acquire()` (UNIQUE/READ_ONLY) or re-probe (READ/WRITE); success →
  build accessor, resume query; miss → re-park; deadline passed → throw timeout exception → Error.

### U3c — glue wiring
- `SessionContext.hpp`/`SessionHL.hpp`: any pool wrappers PendingBegin needs (mirror the
  PendingCommit wrappers already added in U4b).

## U3a-3s — storage worker concrete shapes (grounded)
Facts: `ThrowAccessTimeout(StorageAccessType)` is `[[noreturn]]` in an ANON namespace at storage.cpp:43
(throws UniqueAccessTimeout/ReadOnlyAccessTimeout/SharedAccessTimeout) → must be exposed. `TryAccess`
(inmemory/storage.hpp:819, .cpp:5091) builds `new InMemoryAccessor{this, iso, std::move(guard)}`.
`main_lock_` is a Storage member (storage.hpp:455). PendingScope is NON-movable/non-copyable →
construct in-place in a variant. `storage_access_timeout_sec` default 1s.

1. `storage.hpp`, inside `class Storage` near the Access virtuals (:362-378): nested
   ```cpp
   struct PendingAccess {                      // one in-flight non-blocking BEGIN attempt
     virtual ~PendingAccess() = default;
     virtual std::unique_ptr<Accessor> TryAcquire(std::optional<IsolationLevel> override_isolation_level) = 0;
   };
   virtual std::unique_ptr<PendingAccess> MakePendingAccess(StorageAccessType /*rw_type*/) { return nullptr; }
   ```
2. Expose the timeout throw: move `ThrowAccessTimeout` OUT of the anon namespace in storage.cpp (make it
   `memgraph::storage::ThrowAccessTimeout`), and declare it in storage.hpp near AcquireGuardOrThrow
   (:565): `[[noreturn]] void ThrowAccessTimeout(StorageAccessType rw_type);`. AcquireGuardOrThrow's
   existing call keeps working.
3. `inmemory/storage.hpp`: declare the override
   `std::unique_ptr<PendingAccess> MakePendingAccess(StorageAccessType rw_type) override;` and a PUBLIC
   helper `std::unique_ptr<Accessor> AccessorFromGuard(utils::ResourceLockGuard guard, std::optional<IsolationLevel> iso);`
   (symmetric to TryAccess). Refactor TryAccess to build via AccessorFromGuard (DRY; behavior identical).
4. `inmemory/storage.cpp`: `#include <variant>`. File-local concrete class:
   ```cpp
   namespace {
   class InMemoryPendingAccess final : public Storage::PendingAccess {
    public:
     InMemoryPendingAccess(InMemoryStorage *storage, utils::ResourceLock &main_lock, StorageAccessType rw_type)
         : storage_{storage}, rw_type_{rw_type} {
       switch (rw_type) {                       // register pending up front for the gating modes only
         case StorageAccessType::UNIQUE:    scope_.emplace<utils::UniquePendingScope>(main_lock);   break;
         case StorageAccessType::READ_ONLY: scope_.emplace<utils::ReadOnlyPendingScope>(main_lock); break;
         default: break;                        // READ / WRITE gate nobody → no scope
       }
     }
     std::unique_ptr<Storage::Accessor> TryAcquire(std::optional<IsolationLevel> iso) override {
       switch (rw_type_) {
         case StorageAccessType::UNIQUE: {
           auto g = std::get<utils::UniquePendingScope>(scope_).try_acquire();
           return g ? storage_->AccessorFromGuard(std::move(*g), iso) : nullptr;
         }
         case StorageAccessType::READ_ONLY: {
           auto g = std::get<utils::ReadOnlyPendingScope>(scope_).try_acquire();
           return g ? storage_->AccessorFromGuard(std::move(*g), iso) : nullptr;
         }
         default: return storage_->TryAccess(rw_type_, iso);   // READ/WRITE: plain one-probe
       }
     }
    private:
     InMemoryStorage *storage_;
     StorageAccessType rw_type_;
     std::variant<std::monostate, utils::UniquePendingScope, utils::ReadOnlyPendingScope> scope_;
   };
   }  // namespace
   std::unique_ptr<Storage::PendingAccess> InMemoryStorage::MakePendingAccess(StorageAccessType rw_type) {
     return std::make_unique<InMemoryPendingAccess>(this, main_lock_, rw_type);
   }
   ```
   NOTE: DiskStorage does NOT override → inherits the base `return nullptr` (query layer falls back to
   blocking on disk). Do NOT touch disk/storage.*.

## U3a-3q — query worker (depends on U3a-3s + addendum being green)
Two files. Ground truth: `CurrentDB` struct interpreter.hpp:255-293 (ResetDB :275); SetupDatabaseTransaction
interpreter.cpp:196-231; CleanupDBTransaction interpreter.cpp:233-242. `db_acc->storage()` → `Storage*`
(base); `IsCommitSerialised()` is storage.hpp:408. `BeginWouldBlockException` is in query/exceptions.hpp
(already included by interpreter.cpp). `storage::ThrowAccessTimeout` decl comes from storage.hpp (U3a-3s).

### interpreter.hpp — CurrentDB members (after `metrics::ScopedGauge transaction_gauge_;`, ~:292)
```cpp
  // U3 main_lock BEGIN park (experimental_lockfree_read_snapshot ON). A non-blocking BEGIN attempt that
  // missed once and is now parked/retried by the bolt driver; carries its PendingScope (writer-preference)
  // for its whole life. unique_ptr so teardown here (CleanupDBTransaction/ResetDB/~CurrentDB) deregisters
  // the pending scope on any abandonment — a leaked registration would block a main_lock mode process-wide.
  std::unique_ptr<storage::Storage::PendingAccess> pending_access_;
  std::optional<std::chrono::steady_clock::time_point> pending_begin_deadline_;
```
Add `pending_access_.reset(); pending_begin_deadline_.reset();` to `ResetDB()` (:275-280).

### interpreter.cpp — SetupDatabaseTransaction (:196) rewrite to the Option A″ pseudocode above.
- Keep the existing `if (!db_acc_) throw`, the `db_arena_scope`, and `const auto timeout = GetStorageAccessTimeoutSec();`.
- Insert the flag-gated non-blocking block (fast-path TryAccess → MakePendingAccess → TryAcquire →
  success/timeout/park), then guard the existing `switch(acc_type){Access/UniqueAccess/ReadOnlyAccess}`
  with `if (!db_transactional_accessor_)` so it runs only for flag-OFF / Disk. The post-setup
  (execution_db_accessor_.emplace(:224), transaction_gauge_(:226), triggers(:228)) is UNCHANGED and runs
  after an accessor exists.
- CleanupDBTransaction (:233): also `pending_access_.reset(); pending_begin_deadline_.reset();`
  (belt-and-suspenders for a mid-park abort; the normal success/timeout paths already reset in Setup).
- Needs `#include <chrono>` if not present.

FLAG-OFF / Disk MUST be byte-identical: when `IsCommitSerialised()` is false the whole new block is
skipped and control falls straight into the original switch.

## Timeout semantics (user-locked)
- Commit park: NO timeout (bounded by holder's replication resolution). [done in U4]
- BEGIN park: FINITE timeout = `GetStorageAccessTimeoutSec` — same deadline the blocking path uses
  today. On expiry the driver throws the mode's existing timeout exception. This is the "bring the
  timeout onto BEGIN/main_lock" the user asked for.

## KEY DIVERGENCE FROM U4 (commit-park) — PendingScope must persist across re-drives
U4's retry is STATELESS: on wake the bolt driver re-Pulls → Commit() → TryLockCommit fresh. Nothing
is carried in the session but "retry me".

U3's UNIQUE/READ_ONLY retry is STATEFUL and this is REQUIRED for liveness, not an optimization:
without a held pending registration, a bare try_lock loop registers nothing, so sustained shared
(READ/WRITE) holders keep main_lock SHARED forever and the UNIQUE/READ_ONLY probe starves. The
`unique_pending_count`/`ro_pending_count` from a live PendingScope is what gates NEW shared
acquisitions (can_acquire<READ|WRITE|READ_ONLY> all demand the relevant pending count == 0), draining
the holders so a probe wins. So the PendingScope MUST outlive each re-drive.

=> **Plumbing: Option A″ (FINAL — SUPERSEDES A′ and A; grounded on the base's own design intent).**
The base already ships `InMemoryStorage::TryAccess` (inmemory/storage.hpp:805, one-probe non-blocking,
nullptr on miss, InMemory-only — DiskStorage deliberately has none so a poller learns to block) and
its doc (:812-815) states the intended scheduler-yield pattern verbatim: "A caller that needs to be
preferred while it retries holds a UniquePendingScope or ReadOnlyPendingScope across the whole loop."
So U3 is the caller the base anticipated. We encapsulate the scope + polymorphic accessor construction
behind ONE small storage handle so the query layer never touches main_lock_ and never sees InMemory
types:

```cpp
// base storage.hpp:
struct PendingAccess {                       // one in-flight non-blocking BEGIN attempt
  virtual ~PendingAccess() = default;
  // One non-blocking probe. Returns the built accessor if main_lock_ now admits the requested mode,
  // else nullptr (still registered pending — call again on the next wake). Never blocks, never throws.
  virtual std::unique_ptr<Accessor> TryAcquire(std::optional<IsolationLevel> override_isolation_level) = 0;
};
// base Storage: default = "no non-blocking path, caller must block" (DiskStorage keeps this default).
virtual std::unique_ptr<PendingAccess> MakePendingAccess(StorageAccessType /*rw_type*/) { return nullptr; }
```
InMemoryStorage overrides `MakePendingAccess` to return a concrete PendingAccess holding, for
UNIQUE/READ_ONLY, the matching PendingScope (registers pending immediately, held for the attempt's
whole life); for READ/WRITE, no scope (they gate nobody) — its TryAcquire just calls TryAccess. Either
way TryAcquire builds the InMemoryAccessor from the adopted guard (factor the post-guard construction
out of TryAccess so both share it).

`CurrentDB` gains two members: `std::unique_ptr<storage::PendingAccess> pending_access_;` and
`std::optional<std::chrono::steady_clock::time_point> pending_begin_deadline_;`.

HOT-PATH RULE: BEGIN (incl. uncontended read BEGIN) is the hot path #4685 protects. Do NOT allocate a
PendingAccess handle or register pending on the uncontended path. Probe once with the alloc-free
`TryAccess` first; only on a MISS allocate MakePendingAccess (which registers pending for
UNIQUE/READ_ONLY) and park. => `TryAccess` must be a BASE Storage virtual (default nullptr = "no probe,
block"; DiskStorage inherits it — this is exactly the semantics inmemory/storage.hpp:817 documents).
See "U3a-3s ADDENDUM" below.

`SetupDatabaseTransaction` (interpreter.cpp:196), flag ON (`db_acc->storage()->IsCommitSerialised()`):
```
if (db_acc->storage()->IsCommitSerialised()) {
  if (!pending_access_) {                                      // first drive of this transaction
    if (auto acc = db_acc->storage()->TryAccess(acc_type, override_isolation_level)) {  // uncontended: NO alloc
      db_transactional_accessor_ = std::move(acc);
    } else {                                                   // contended: begin a pending+parked attempt
      pending_access_ = db_acc->storage()->MakePendingAccess(acc_type);   // nullptr on Disk
    }
  }
  if (pending_access_) {                                       // a parked attempt is in flight (this or prior drive)
    if (!pending_begin_deadline_) pending_begin_deadline_ = steady_clock::now() + GetStorageAccessTimeoutSec();
    if (auto acc = pending_access_->TryAcquire(override_isolation_level)) {
      db_transactional_accessor_ = std::move(acc);
      pending_access_.reset(); pending_begin_deadline_.reset();
    } else if (steady_clock::now() >= *pending_begin_deadline_) {
      pending_access_.reset(); pending_begin_deadline_.reset();
      storage::ThrowAccessTimeout(acc_type);                   // same client-visible timeout as today
    } else {
      throw BeginWouldBlockException{*pending_begin_deadline_};// park until deadline
    }
  }
}
if (!db_transactional_accessor_) {                             // flag OFF, or Disk (TryAccess & MakePendingAccess null)
  // the existing blocking switch: db_acc->Access/UniqueAccess/ReadOnlyAccess(..., timeout)
}
// then the existing post-setup (execution_db_accessor_.emplace, transaction_gauge_, triggers) runs
// only once an accessor was actually acquired (every would-block/timeout path throws before here).
```
Uncontended cost when flag ON: one `TryAccess` (a single try_lock_shared) and NO heap alloc / NO pending
registration — same order as today's `try_lock_for`. Alloc + pending registration happen ONLY under
contention (the cold DDL/UNIQUE/READ_ONLY-vs-txn path).

## U3a-3s ADDENDUM — promote TryAccess to a base Storage virtual
`TryAccess` is currently an InMemoryStorage-only concrete method. Promote it to
`virtual std::unique_ptr<Accessor> TryAccess(StorageAccessType rw_type, std::optional<IsolationLevel> = {}) { return nullptr; }`
on base Storage (default nullptr — the documented "Disk has no probe, so the caller learns to block"),
and mark InMemoryStorage's as `override`. DiskStorage does NOT override. This lets the query layer call
`db_acc->storage()->TryAccess(...)` polymorphically without a dynamic_cast.

Q1c is STRUCTURAL: `pending_access_` is a `unique_ptr` CurrentDB member. It is reset on the success and
timeout paths, and destroyed by CurrentDB teardown (CleanupDBTransaction/ResetDB/ResetInterpreter/
~CurrentDB) on disconnect/RESET — the concrete PendingAccess dtor destroys its PendingScope →
unregister_pending. No bolt-layer reset discipline; no scope in the session or the exception.
ALSO reset `pending_access_` + `pending_begin_deadline_` in `CleanupDBTransaction` and `ResetDB`
(belt-and-suspenders for a mid-park abort).
Q4b: the finite deadline lives in CurrentDB and is carried in BeginWouldBlockException → the bolt park
uses it; the query-layer `now >= deadline` check is the authoritative timeout regardless.
main_lock_ never leaks to the query layer; Disk & flag-OFF stay byte-identical.

Verifier mapping (all preserved/strengthened): Q1a/b unchanged (TryAcquire wraps scope.try_acquire);
Q2 unchanged (PendingAccess created on one worker, TryAcquire'd on the next — sequential, scope ops
take mtx fresh); Q3/Q5 unaffected.

--- superseded ---
Option A′ (do NOT implement): scope+deadline in CurrentDB directly (query layer touches main_lock_).
Option A (do NOT implement): The bolt session's pending-begin state OWNS the scope
The scope + deadline live in `CurrentDB` (the query layer where SetupDatabaseTransaction already is),
NOT in the bolt session. `CurrentDB` persists across the park (the txn is mid-BEGIN, not torn down),
so it can own `std::variant<std::monostate, UniquePendingScope, ReadOnlyPendingScope>
pending_begin_scope_` + `std::optional<steady_clock::time_point> pending_begin_deadline_`.

`SetupDatabaseTransaction` (interpreter.cpp:196), flag ON, becomes a re-entrant probe:
  - Fresh (deadline nullopt): set `deadline = now + GetStorageAccessTimeoutSec()`; for UNIQUE/READ_ONLY
    construct the matching PendingScope into the member; for READ/WRITE leave monostate.
  - Probe: UNIQUE/READ_ONLY → `scope.try_acquire()`; READ/WRITE → `ResourceLockGuard(main_lock_, type,
    std::try_to_lock)`.
    - success → build Accessor from the adopted guard; reset scope=monostate, deadline=nullopt; return.
    - miss & now >= deadline → reset scope+deadline; `ThrowAccessTimeout(rw_type)` (the SAME exception
      today's blocking timeout throws — identical client-visible behavior). [handles Q4b at query layer]
    - miss & now < deadline → throw `BeginWouldBlockException{deadline}` (park until deadline).
  - OFF path: unchanged — the existing AcquireGuardOrThrow(blocking-with-timeout) call.

Q1c is now STRUCTURAL: the scope is a CurrentDB member, so its destructor runs `PendingScope::
~PendingScope` (→ unregister_pending) whenever CurrentDB dies (session disconnect / RESET /
ResetInterpreter), and the success + timeout paths reset it explicitly in SetupDatabaseTransaction.
No bolt-layer OnScopeExit reset discipline is required (the verifier's Q1c fix was for Option A).
main_lock_ outlives the scope because CurrentDB holds db_acc_ (keeps the database alive) for the
scope's whole lifetime.

The bolt driver (U3b) is then as thin as commit-park: catch BeginWouldBlockException → park with the
FINITE stashed deadline → on wake re-drive the query (which re-enters SetupDatabaseTransaction). No
scope in the session, no scope in the exception.

--- superseded ---
Option A (do NOT implement): The bolt session's pending-begin state OWNS the scope
(`std::variant<std::monostate, UniquePendingScope, ReadOnlyPendingScope>`; monostate for READ/WRITE).
On the first miss SetupDatabaseTransaction (or a storage helper it calls) constructs the scope, probes
once, and on miss the scope is MOVED OUT into the session state (via the stash), not destroyed. On
each re-drive the session passes the held scope back down; the probe is `scope.try_acquire()`. On
success the returned guard is adopted into the Accessor ctor (Accessor already takes a
ResourceLockGuard); the scope goes inert. Storage stays OFF-path byte-identical: the non-blocking
probe + scope construction is a NEW flag-gated branch, the existing AcquireGuardOrThrow path
(storage.cpp:62) is untouched when the flag is OFF.

Storage-side choke point: `AcquireGuardOrThrow(storage, rw_type, timeout)` (storage.cpp:62) is the
single main_lock acquisition site behind Storage::Access/UniqueAccess/ReadOnlyAccess. The new
non-blocking sibling lives beside it. READ/WRITE probe = `ResourceLockGuard(main_lock_, type,
std::try_to_lock)`; UNIQUE/READ_ONLY probe = `scope.try_acquire()`. The Accessor is built from the
adopted guard exactly as the blocking path builds it.

Flag gate: `config_.experimental_lockfree_read_snapshot` (== `Storage::IsCommitSerialised()`,
storage.hpp:408) — the same gate commit-park uses.

## U3b — bolt PendingBegin park/resume driver (EXACT MIRROR of PendingCommit; grounded)
The in-tree PendingCommit driver (U4b/U4c) is the template — read it and mirror it. Two throw origins,
mapping 1:1 to PendingCommit's two:
- **RUN origin** ↔ U4 PULL origin: `BeginWouldBlockException` throws from `session.InterpretPrepare()`
  inside `HandlePrepare` (handlers.hpp:225). Retry = re-run `HandlePrepare(impl)` (re-Prepares, sends header).
- **BEGIN-message origin** ↔ U4 COMMIT-message origin: throws from `session.BeginTransaction(extra)` inside
  `HandleBegin` (handlers.hpp:448). Retry = re-run `impl.BeginTransaction(extra)` + `MessageSuccess({})`.

### state.hpp
- Add `State::PendingBegin` (mirror `State::PendingCommit` at :71 + its doc).
- Add `enum class PendingBeginOutcome : uint8_t { Reschedule, Done, ClientError };` next to PendingCommitOutcome
  (must live here so v2/session.hpp can name it).

### bolt/v1/session.hpp — mirror the PendingCommit surface (lines 163-276, 323-328)
- Execute_ dechunk loop: after the `if (state_ == State::PendingCommit) return true;` block (:163-168) add
  `if (state_ == State::PendingBegin) { return true; }` (stop the loop so a pipelined msg can't run first).
- `bool HasPendingBegin() const { return pending_begin_; }`
- `void StashPendingBeginPrepare(std::chrono::steady_clock::time_point deadline) { pending_begin_ = true;
   pending_begin_from_message_ = false; pending_begin_deadline_ = deadline; }`
- `void StashPendingBeginMessage(<extra-map-type> extra, std::chrono::steady_clock::time_point deadline)
   { pending_begin_ = true; pending_begin_from_message_ = true; pending_begin_extra_ = std::move(extra);
     pending_begin_deadline_ = deadline; }`  (extra-map-type = the type of `extra.ValueMap()` that HandleBegin
     passes to BeginTransaction — determine by reading; it is the bolt Value map, e.g. `map_t`.)
- `std::chrono::steady_clock::time_point PendingBeginDeadline() const { return pending_begin_deadline_; }`
- `template <typename TImpl> PendingBeginOutcome FinishPendingBegin_(TImpl &impl)` mirroring
  FinishPendingCommit_ (:227-276):
  ```
  MG_ASSERT(pending_begin_, ...);  ScopedSessionLog log_guard(impl.GetLogContext());
  if (pending_begin_from_message_) {
    try {
      impl.BeginTransaction(pending_begin_extra_);
      if (!encoder_.MessageSuccess({})) { state_ = State::Close; pending_begin_ = false; return ClientError; }
      state_ = State::Idle; pending_begin_ = false; return Done;
    } catch (const memgraph::query::BeginWouldBlockException &) {
      return PendingBeginOutcome::Reschedule;         // pending_begin_ stays true; extra + deadline retained
    } catch (const std::exception &e) {
      state_ = HandleFailure(impl, e); pending_begin_ = false; return ClientError;  // e.g. access timeout
    }
  }
  state_ = details::HandlePrepare(impl);              // RUN origin: re-Prepare (sends header on success)
  switch (state_) {
    case State::PendingBegin: return PendingBeginOutcome::Reschedule;  // BeginWouldBlock again; HandlePrepare re-stashed
    case State::Result:       pending_begin_ = false; return PendingBeginOutcome::Done;
    default:                  pending_begin_ = false; return PendingBeginOutcome::ClientError;  // Error/Close: HandleFailure sent it
  }
  ```
  NOTE vs U4c: the message reschedule does NOT re-call Stash (unlike U4c:244) — pending_begin_ is already
  true and pending_begin_extra_ must be retained (re-stashing would self-move the map). Just return Reschedule.
- Members (mirror :323-328):
  `bool pending_begin_{false}; bool pending_begin_from_message_{false}; <extra-map-type> pending_begin_extra_{};
   std::chrono::steady_clock::time_point pending_begin_deadline_{};`

### bolt/v1/states/handlers.hpp — two catches, each BEFORE the generic `catch (const std::exception &e)`
- `HandlePrepare` (:244): `} catch (const memgraph::query::BeginWouldBlockException &e) {
     session.StashPendingBeginPrepare(e.deadline()); return State::PendingBegin; }`
- `HandleBegin` (:454): `} catch (const memgraph::query::BeginWouldBlockException &e) {
     session.StashPendingBeginMessage(extra.ValueMap(), e.deadline()); return State::PendingBegin; }`
  (query/exceptions.hpp is already included here — CommitWouldBlockException is caught in this file.)

### communication/v2/session.hpp — mirror PostFinishPendingCommit (442-483), DoWork branch (398-413),
###   drain loop (~387), and the task-id member (607)
- DoWork: after the HasPendingCommit branch, add
  `if (shared_this->session_.HasPendingBegin()) { shared_this->PostFinishPendingBegin(); return; }`.
- Teardown drain loop (the `while (session_.HasPendingCommit()) session_.FinishPendingCommit();` at ~:387):
  add an analogous `while (session_.HasPendingBegin()) session_.FinishPendingBegin();` beside it.
- `PostFinishPendingBegin()` mirrors PostFinishPendingCommit but: calls `session_.FinishPendingBegin()`;
  switches on PendingBeginOutcome; on Reschedule re-park; uses `pending_begin_task_id_`; and the
  **ParkAdmission deadline is FINITE**: `session_.PendingBeginDeadline()` (NOT time_point::max — Q4b),
  tag `utils::WaitTag{utils::WaitResource::MainLock, {}}`.
- `std::atomic<utils::PriorityThreadPool::TaskID> pending_begin_task_id_{0};`

### glue/SessionHL.hpp — mirror FinishPendingCommit() (:147)
- `memgraph::communication::bolt::PendingBeginOutcome FinishPendingBegin() { return this->FinishPendingBegin_(*this); }`
  (BeginTransaction is already exposed at SessionHL.hpp:72; ParkAdmission/IsDrainingAdmissions already in
  SessionContext.hpp from U1.)

### KEY CORRECTNESS PROPERTY to verify post-implementation (like U4's re-Pull safety)
Re-running `HandlePrepare`/`InterpretPrepare` after a BeginWouldBlock throw from SetupDatabaseTransaction
must be idempotent: no non-idempotent side effect may occur in Interpreter::Prepare BEFORE the accessor is
acquired in SetupDatabaseTransaction. (Accessor acquisition is where the throw happens, so nothing storage-
mutating has run.) Flag for the concurrency/logic review of concrete U3.

## U3c — wire main_lock notify hook → WakeMatching({MainLock}) (NEEDED, not optional)
Why needed: the perf kit's UNIQ_RO/UNIQ_UQ scenarios exercise main_lock directly. Base #4685 blocks on
main_lock but wakes INSTANTLY (ResourceLock's own cv.notify_all). If U3's parked tasks wake only via the
100ms monitor backstop, U3 would be a large regression in exactly those scenarios. So the on_notify_all_
hook must fire WakeMatching so parked tasks wake as promptly as the cv did. (U3's real value is
long-DDL-under-load: frees the pool threads that would otherwise all block through a long CREATE INDEX;
the hook makes their post-DDL resume prompt.)

DATA-RACE FIX (found during U3c grounding): lazy install writes the hook while concurrent maybe_notify
calls READ it off-mtx → a move_only_function assign-vs-call race (UB). So the U3a hook (plain
move_only_function read directly in maybe_notify) must be revised to ATOMIC-POINTER PUBLICATION in
resource_lock.hpp:
```cpp
 private:
  std::move_only_function<void()> on_notify_all_storage_;               // owns the callable; set once
  std::atomic<std::move_only_function<void()>*> on_notify_all_{nullptr};// published pointer (nullptr = unarmed)
 public:
  void SetNotifyHook(std::move_only_function<void()> hook) {            // install once, before or during concurrent use
    on_notify_all_storage_ = std::move(hook);
    on_notify_all_.store(on_notify_all_storage_ ? &on_notify_all_storage_ : nullptr, std::memory_order_release);
  }
  void ClearNotifyHook() { on_notify_all_.store(nullptr, std::memory_order_release); }  // storage_ left valid until ~ResourceLock
```
maybe_notify:
```cpp
  if (kind == NotifyKind::All) {
    cv.notify_all();
    if (auto *h = on_notify_all_.load(std::memory_order_acquire)) (*h)();  // release/acquire: storage write is visible
  }
```
Race-free because: install is done AT MOST ONCE (Storage-level exchange guards it) and publishes the
callable via release/acquire; ClearNotifyHook publishes nullptr but never destroys storage_ (so an
in-flight reader that loaded the old pointer still derefs valid storage_); ~ResourceLock runs only after
all main_lock activity has ceased (DB torn down), so no notify is in flight at destruction.

Design (lazy install — avoids startup-order + new/resumed-DB seam + is uniform across all DBs):
1. base Storage (storage.hpp, near main_lock_ :455): add
   ```cpp
   std::atomic<bool> main_lock_hook_installed_{false};
   bool MainLockHookInstalled() const noexcept { return main_lock_hook_installed_.load(std::memory_order_acquire); }
   // Install once; a second call is a no-op (returns false). The exchange makes concurrent first-callers safe.
   bool TrySetMainLockNotifyHook(std::move_only_function<void()> hook) {
     if (main_lock_hook_installed_.exchange(true, std::memory_order_acq_rel)) return false;
     main_lock_.SetNotifyHook(std::move(hook));
     return true;
   }
   void ClearMainLockNotifyHook() { main_lock_.ClearNotifyHook(); main_lock_hook_installed_.store(false, std::memory_order_release); }
   ```
2. Interpreter layer — `Interpreter::SetupDatabaseTransaction` (interpreter.cpp:11270 wrapper; has
   `interpreter_context_` and `current_db_`). Both BEGIN-message and RUN paths route through it. Before the
   `current_db_.SetupDatabaseTransaction(...)` call, flag-gated, install once per storage (cheap load-guard
   avoids constructing the lambda every BEGIN; the exchange inside Try* closes the load→install race):
   ```cpp
   if (current_db_.db_acc_) {
     auto *storage = (*current_db_.db_acc_)->storage();
     if (storage->IsCommitSerialised() && interpreter_context_->worker_pool && !storage->MainLockHookInstalled()) {
       storage->TrySetMainLockNotifyHook([pool = interpreter_context_->worker_pool] {
         pool->WakeMatching(utils::WaitTag{utils::WaitResource::MainLock, {}});
       });
     }
   }
   ```
   (utils::WaitTag/WaitResource already used in this TU by U4's WakeMatching({CommitLock}).) Covers default,
   newly-created, and resumed tenants uniformly, at the first txn that touches each.
3. SHUTDOWN UAF (worker_pool_ declared memgraph.cpp:869 AFTER dbms_handler :810 → destroyed BEFORE the
   databases; a main_lock release during DB teardown would fire the hook on a dead pool). FIX: clear all
   hooks before pool destruction, at the existing shutdown `dbms_handler->ForEach` (memgraph.cpp:1147, runs
   after worker_pool_->ShutDown() :1123 and before end-of-main pool destruction). Add inside that lambda:
   `acc->storage()->ClearMainLockNotifyHook();`. Between :1123 and the clear, a notify may still fire on the
   shut-down-but-ALIVE pool → WakeMatching sees draining_ → safe no-op; after the clear, no hook fires.
   VERIFY in the concurrency review that :1147 runs on the normal shutdown path before pool destruction.

## Wake correctness
`WakeMatching({MainLock})` wakes ALL MainLock-parked tasks on ANY main_lock NotifyKind::All (decision
#2: per-resource conservative wake, correct-by-construction, mirrors the lock's own notify_all). Mode
is carried in the tag for a future per-mode narrowing only. `has_parked_` relaxed pre-filter;
100ms monitor sweep is the accepted lost-wakeup backstop.

## Storage access modes in BEGIN — VERIFIED (2026-09-04)
Full record: `~/workspace/opencode-work/commit-lock-scheduling/2026-09-04--storage-access-modes-in-begin.html`.
Verdict: **all four modes correct, no fix required.** The park is transparent to `main_lock_`'s admission
and writer-preference semantics for every access type that reaches BEGIN.

- `StorageAccessType` has FIVE values (access_type.hpp:18): UNIQUE, WRITE, READ, READ_ONLY, NO_ACCESS.
  `NO_ACCESS` is structurally excluded from the park: `CurrentDB::accessor_type_` (a `std::optional`) is
  NEVER assigned NO_ACCESS, meta queries leave it nullopt, and Prepare only calls SetupDatabaseTransaction
  when it is engaged (interpreter.cpp:10839). So `ToGuardType(NO_ACCESS)`'s `LOG_FATAL` (storage.hpp:75-76)
  is unreachable on the flag-ON TryAccess path — same as the base Access. Flag-OFF-identical preserved.
- The four reachable modes and their sources: UNIQUE = DDL (point/text/vector index, DROP ALL, DROP GRAPH,
  enum, disk-constraint, RECOVER SNAPSHOT — interpreter.cpp:10437,10480-10508,10593); READ_ONLY =
  CREATE INDEX / CREATE CONSTRAINT (in-memory) / edge-index create (interpreter.cpp:10554,10560,10577,10593);
  WRITE = general Cypher writes (RWType W/RW, :3888-3895) + explicit BEGIN write (:3974); READ = read queries
  (EXPLAIN/DUMP/ANALYZE/DESCRIBE-get, index-drop) + explicit BEGIN read.
- **Correctness invariant:** U3's per-mode scope split mirrors the base ResourceLock's per-mode pending
  registration EXACTLY — only UNIQUE (`unique_pending`) and READ_ONLY (`ro_pending`) register when blocking
  (resource_lock.hpp:80-81); READ/WRITE register nothing (:77). So the park holds `UniquePendingScope` /
  `ReadOnlyPendingScope` for those two, and NO scope for READ/WRITE. A parked task therefore carries the
  same admission priority a blocking acquirer of that mode would have. Wake (`WakeMatching({MainLock})`) +
  finite BEGIN deadline cover all four uniformly.
- **Nuance 1 (judgment call, NOT a bug):** the alloc-free fast-path probe does not register pending for
  UNIQUE/READ_ONLY — only the on-miss `MakePendingAccess` does. vs the base (register-then-wait), this is a
  one-probe window of softened writer-preference on the cold DDL path; once parked, the scope persists and
  gating is full (no starvation). Kept for the uniform fast path; tightening = route UNIQUE/READ_ONLY
  straight to MakePendingAccess (adds an alloc to every uncontended DDL BEGIN). OPEN DECISION.
- **Nuance 2 (covered):** a `SET STORAGE MODE` mid-park can flip a constraint's mode (READ_ONLY↔UNIQUE).
  The pre-existing `StorageModeChangedDuringSetupException` retry (interpreter.cpp:10845-10853) handles it —
  SetupDatabaseTransaction resets `pending_access_` on the successful-acquire path (:218) BEFORE the pinned-
  mode check throws, so the retry re-derives the correct mode with a fresh `pending_access_`. Park doesn't
  break it.

## Liveness + task-closure audits (2026-09-04) — outcomes + L1 fix
Two adversarial concurrency-debugger passes over the whole park machinery (pool + commit-park + main-lock-
park + notify hook + monitor + shutdown), folded into commit `1a5acf74c`.

- **Liveness (lost-task / missed-wake / block-forever):**
  - L2 (no missed wake) PASS — monitor's unconditional kick-oldest (every 100ms, ignores tag+deadline)
    backstops the deadline-less commit parks; `has_parked_` relaxed-load race ≤100ms.
  - L3 (no block forever) PASS — lock order acyclic (`parked_mtx_`→`Worker::mtx_` only; ResourceLock mtx
    released before the hook fires; commit_mutex_/engine_lock_ released before WakeMatching); no park-induced
    starvation beyond std::mutex; PendingScope released by the finite BEGIN deadline; shutdown can't strand.
  - **L1 (no lost task) — REAL bug FOUND + FIXED (`1a5acf74c`).** Shutdown-only race: `WakeMatching` pulls a
    parked continuation out of the deque, `ShutDown` then swaps + `request_stop()`s, and
    `ScheduledReAddTask`'s stop-guard silently DROPPED it (client got a TCP close, not a Bolt error). No hang/
    corruption, but a genuine lost task. Closed at both ends by running the woken continuation INLINE
    (mirrors ShutDown's own drain; the v2 drivers short-circuit via `IsDrainingAdmissions()` at
    v2/session.hpp:464,510): `WakeMatching` runs inline if draining; `ScheduledReAddTask` runs inline on
    `stop_requested` instead of dropping. Flag-OFF unaffected. Pool 27/27, bolt_session 35/35, binary links.
- **Task-closure audit (the pool-scheduled lambdas):** T1 capture-lifetime, T2 exactly-once, T3 execution-
  context, T4 session-state concurrency — ALL PASS. Every lambda captures `shared_from_this()` (no UAF across
  a seconds-long park); ≤1 in-flight task per session (no double-commit/response/state race). One physically-
  unrealizable + 100ms-self-recovering NOTE on the `pending_*_task_id_` relaxed store — left relaxed, comment
  corrected in `1a5acf74c`.
