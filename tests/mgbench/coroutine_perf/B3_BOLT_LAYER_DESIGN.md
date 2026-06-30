# B-sequential Bolt layer — make a parked pull free the worker (design spec for verification)

Branch `feat/coroutine-scheduler-redesign` @ `1ea004ae3` (core done: PullPlan::Pull can park/resume,
dormant). This layer turns a `PullOutcome::Parked` into **"the query's pool task returns → the worker is
freed → the continuation is rescheduled (pinned to the same worker) → on resume it re-drives the pull."**
It enables BOTH yield (scheduler-requested suspend) and park (coordinator waiting on branches) because both
bottom out in the same parked-return; they differ only in the resume trigger.

> Option B (decouple query execution into its own resumable pool task), strictly SEQUENTIAL per session.
> Option A (thread Parked through the shared Bolt protocol state machine + decode-bypass) was rejected:
> higher blast radius on shared bolt v1/v4/v5 code.

## ⚑ VERIFICATION-CLEARED REVISIONS (2026-06-30 — concurrency-debugger + skeptic converged)
Verdict: core insight SOUND + implementable, but two specced assumptions were WRONG and a set of
concurrency mechanisms MUST be added. These supersede the prose below where they conflict.

**CORRECTION 1 — mode is NOT knowable at the dispatch site (skeptic GF7/D3, REFUTED).** `cursor_->mode()`
is buried 3 layers inside the move-only `query_handler` closure (PullPlan→query_handler→PreparedQuery);
`HandlePullDiscard` only has `SessionHL`. So the coro-vs-sync branch needs new plumbing, set ONCE at
PullPlan construction (where `cursor_->mode()` is known, interpreter.cpp:3398-3404):
  - add `bool is_coro_driven{false}` to `PreparedQuery` (interpreter.hpp:~234), set in `PrepareCypherQuery`
    from the root cursor mode (`false` for all non-Cypher / PullPlanVector handlers — they never park).
  - `Interpreter::IsCursorCoroDriven(std::optional<int> qid)` → reads `query_executions_[qid]->prepared_query->is_coro_driven`.
  - `SessionHL::IsPullCoroDriven()` → forwards to the interpreter.
  - `HandlePullDiscard` (free fn taking `TSession&`) calls `session.IsPullCoroDriven()` to branch.

**CORRECTION 2 — the "stop DoWork" signal must NOT change `Execute_`'s return type.** `Execute_` returns
`bool`; widening it (or adding a `State`) bleeds into shared bolt v1/v4/v5 protocol code = Option A's blast
radius (the thing B avoids). Instead use a **side-channel flag on SessionHL** (NOT on `bolt::Session`):
`bool pull_task_dispatched_`. Set it in the HandlePull path when a coro pull is dispatched; the DoWork
lambda reads it after each `Execute()` and, if set, returns WITHOUT DoRead and WITHOUT the priority-
reschedule.

**Dispatch-stop nuance (pipelining):** HandlePull does NOT call Pull; it stashes `(n,qid,is_pull)` on
SessionHL, sets `pull_task_dispatched_`, and returns. But `Execute_`'s loop would otherwise `GetChunk()`
the NEXT buffered message and process it inline before DoWork ever sees the flag — breaking sequential for
a pipelined client (RUN+PULL+RUN+PULL in one TCP segment). So `Execute_` needs a MINIMAL additive guard:
after the per-message switch, `if (impl.PullTaskDispatched()) return;` (break the decode loop immediately).
This reads an impl-side flag (Execute_ already calls impl methods) — it is NOT a return-type/State change,
so the shared bolt v1/v4/v5 protocol semantics are untouched (Correction-2 intent preserved). DoWork then
sees the flag and dispatches. The DISPATCH itself lives in DoWork (it creates the pull-task closure
capturing shared_this + the stashed n/qid — DoWork can reach the private DoWork/DoRead/pull_in_flight_;
HandlePull cannot, so HandlePull only stashes + flags). The pull-task does the HandlePull TAIL
(MessageSuccess + set Bolt State) itself, on completion, before re-arm.

**REQUIRED CONCURRENCY MECHANISMS (all in v2 Session / SessionHL — pool infra B1 is sufficient):**
- **`std::atomic<bool> pull_in_flight_` on `Session`.** Set (release) before dispatching the pull-task;
  the strand-bound `OnRead` and the DoWork priority-reschedule branch (session.hpp:397-400) MUST check it
  (acquire) and return early if set, so no SECOND pool task / no DoRead is armed while the pull-task runs.
  Cleared by the pull-task's re-arm (sequenced before it posts the next DoWork). This is THE serialization
  guarantee; pair with a release-build assert (flips 0→1 on pull-task entry, 1→0 on exit; never >1).
- **Re-arm = pinned + unconditional + LAST.** The pull-task re-arms via `RescheduleTaskOnWorker` (pinned,
  same worker — never `ScheduledAddTask`, which another worker could steal and race the tail). It calls
  `DoWork()` UNCONDITIONALLY (DoWork internally calls Execute→empty buffer→DoRead); do NOT branch on
  buffer state (`input_buffer_` is private + racing the strand). The re-arm + `pull_in_flight_=false` are
  the LAST writes, sequenced-before the pinned push.
- **Pull-task body always re-enters via `SessionHL::Pull`** (→ Interpreter::Pull → PullPlan::Pull) on EVERY
  invocation (fresh AND park-resume) — NOT a raw `handle.resume()` — so `DbArenaScope` (Interpreter::Pull)
  + `StartTrackingCurrentThread` (PullPlan::Pull) are re-established each time. On resume, re-arm tracking
  BEFORE the OOM `SetCounter(park_saved_oom_)` (c-core-2 ordering fix).
- **The pull-task is a closure CAPTURED INSIDE `DoWork`** (so it can reach the private `DoWork`/`DoRead`/
  `pull_in_flight_`/`session_`). It is dispatched via `session_context_->AddTask`/`ScheduleResumableTask`.

**CONFIRMED SOUND (no change needed):** HIGH-priority + coro is STRUCTURALLY IMPOSSIBLE (PrepareCypherQuery
hardcodes Priority::LOW @interpreter.cpp:3917; only Cypher uses PullPlan/coro) → the LOW-only resumable
assert is satisfied by construction, no runtime guard needed (document it). `has_more`(BatchContinues) vs
`Parked` never conflate (a BatchContinues pull-task already returned DONE; a fresh next-PULL is a new
dispatch, a park-resume re-enters the same task). Sequential invariant IS achievable with the above.

**d2-ONLY (parks dormant in d1, so deferred):** shutdown-while-parked — `ShutDown()` must `NotifyAll()` all
live `WorkerResumeEvent` waiters (else a parked continuation holding `shared_from_this` leaks the Session +
suspended coroutine frame) + handle the `DoShutdown`-vs-socket-write TOCTOU (Write already converts a
closed-socket send to OnError, but the concurrent close on the fd is UB → gate the write or drain on
shutdown). Ties to the c-core-2 `~PullPlan` shutdown guard.

## Ground facts (verified)
- `Session::DoWork()` (communication/v2/session.hpp:390) posts ONE pool task: `while (session_.Execute()) {...}`;
  when `Execute()` returns false (buffer drained) it calls `DoRead()` (async read, strand-bound) and the
  task returns. So today: one in-flight pool task per session; decode + pull + encode all inline on it.
- A PULL is handled deep in `Execute_` (bolt/v1/session.hpp) → `HandlePullDiscard` (handlers.hpp:107) →
  `SessionHL::Pull` (glue/SessionHL.cpp:372) → `Interpreter::Pull` → `PullPlan::Pull` (streams rows to the
  encoder/socket inline; socket write is unlocked — communication/v2/session.hpp:147-177).
- `strand_` serializes only the asio io callbacks (OnRead/DoRead). The pool task is NOT on the strand; it
  writes the socket directly. SAFE today only because exactly one task touches the session at a time.
- `interpreter_` is a Session member (persists across pulls). `Interpreter::Pull` already has the
  `bool *parked` out-param (c-core-1); on Parked it writes nothing/no-commit (c-core-2).

## Scope decision: dispatch ONLY coro-driven pulls as resumable tasks
A pull can only park if its root cursor is Coro-driven (the split policy). So:
- **Coro pull** → dispatch as a `ScheduleResumableTask`; can park/yield. (NEW path.)
- **Sync pull** (root Sync, e.g. empty knob, or non-Cypher PullPlanVector handlers) → inline, exactly as
  today. Zero overhead / zero behavior change for the sync hot path. (Yield only ever applies to the coro
  region anyway — a sync subtree cannot yield.)
The "is this pull coro-driven" predicate is the same `cursor_->mode()==Coro && !is_profile_query` that
PullPlan::Pull already computes; expose it so the dispatch site can branch.

## The flow (sequential — at most one task touches a session at any instant)
```
DoWork pool task on worker W:
  Execute_()  // decode buffered Bolt messages
    ... reaches a PULL(n,qid) whose query is CORO-driven:
      → instead of calling Pull inline, DISPATCH a resumable pull-task (pinned-capable) and
        SIGNAL "stop the decode loop, do NOT DoRead — the pull-task owns continuation".
      → Execute_ returns in a way that makes DoWork STOP (return without DoRead).  W is now free
        (the decode task ended); the dispatched pull-task is queued.
  [W picks up the pull-task — same worker, sequential]
pull-task body (ScheduleResumableTask, resumable bool()):
  parked=false; summary = interpreter_.Pull(&stream, n, qid, &parked)   // streams rows to encoder
  if parked:
      return PARK   // wrapper returns → W freed; resume arranged (see Resume kinds). On resume the
                    // wrapper re-invokes this body → interpreter_.Pull resumes the stashed coro.
  else:  // Finished or BatchContinues
      do the HandlePull TAIL: encoder_.MessageSuccess(summary); set Bolt State (Result if has_more
        else Idle);  // exactly what HandlePullDiscard does today after Pull returns
      RE-ARM: post a fresh DoWork pool task (to drain any remaining buffered messages) or DoRead if the
        buffer is empty — i.e. hand control back to the normal decode loop. Then return DONE.
```
The pull-task is the ONLY thing touching the session/socket between dispatch and re-arm (decode stopped,
no DoRead armed) → no concurrent socket write, no concurrent state mutation. The re-arm posts the next
task ONLY after the pull-task is fully done (summary sent, state set).

## Resume kinds — the 1-bit distinction (enables yield AND park on one mechanism)
A parked-return carries WHY it parked, so the pool knows how to resume:
- **Yield** (scheduler set `yield_requested`; `YieldPointAwaitable` suspended): the continuation should be
  **rescheduled immediately, pinned to W** (it runs after any higher-priority task ahead of it drains).
  This is `ScheduleResumableTask`'s self-reschedule path (B1).
- **Park-on-event** (the parallel coordinator `co_await`ed "branches done?"): the continuation must NOT be
  rescheduled now — it waits for `WorkerResumeEvent::NotifyAll`/`NotifyProgress` (fired by the last branch)
  to re-enqueue it pinned (B1).
Carry the kind on the parked path (e.g. an enum on the `parked` signal, or a ctx flag set by the awaitable
that suspended). In THIS layer only the Yield kind is reachable (park-on-event needs c3's coordinator
restructure); but wire the distinction now so c3 is additive.

## Binding invariants (concurrency-critical — this layer is multi-threaded)
- **One task per session:** decode dispatches the pull-task and STOPS; the pull-task re-arms exactly once on
  completion. Never two tasks for one session concurrently. Add a release assert (e.g. an atomic
  `in_flight_per_session` flips 0→1 on task entry, 1→0 on exit; assert never >1) + TSan.
- **Socket/encoder single-writer:** only the in-flight task writes; DoRead is not armed during a pull. The
  strand still owns reads. No lock added (sequential makes it unnecessary) — VERIFY no path arms DoRead or
  posts a second task while a pull-task is in flight.
- **Lifetime:** the pull-task captures `shared_from_this()` (as DoWork already does), so the Session +
  its `interpreter_` outlive the task; a parked task holds the session alive across the park gap.
  On connection drop while parked: the WorkerResumeEvent/pinned continuation must still run (or be
  cancelled) without UAF — VERIFY shutdown-while-parked (mirrors ~PullPlan shutdown guard from c-core-2).
- **Priority:** resumable tasks must be LOW (B1 MG_ASSERT). A HIGH-priority query (ApproximateQueryPriority)
  is NOT park-capable → such a query takes the inline sync path (or runs as a non-resumable task). Confirm
  the dispatch site only uses ScheduleResumableTask for LOW + coro pulls.
- **TLS across park** travels via the resumable-task wrapper scope (B1 CurrentResumableTaskScope) + the
  c-core-2 OOM counter save/restore — this is the layer where they finally matter (another query runs on W
  while parked). VERIFY arena/mem-tracking/OOM all restore on resume.

## Staged implementation
- **d1 — dispatch coro pulls as a resumable task, NO park yet.** Route coro PULLs through
  ScheduleResumableTask + the re-arm, but keep production with park dormant (nothing sets yield_requested).
  Gate: full unit + e2e green; sync pulls byte-identical; coro pulls produce identical results (just run on
  a dispatched task instead of inline). One-task-per-session assert + TSan on the dispatch/re-arm.
- **d2 — wire the yield trigger end-to-end (DEBUG seam first).** Make `Parked` actually free W + resume:
  on a yield-kind park the wrapper self-reschedules pinned. Prove with the DEBUG force-yield seam through
  the REAL session/pool path (not just the interpreter unit test): a force-yielding query parks, W runs
  another task, the query resumes pinned + completes with identical results. TSan.
- **d3 — production yield trigger.** Have the scheduler/monitor set `yield_requested` on a worker with
  queued HP work behind a running coro task (demand-based). Bring-up behind a flag; measure.
- (**c3 — parallel coordinator park** is the separate parallel-execution restructure; rides this layer.)

## Test plan
- d1: a Bolt-level test (e2e or a session-level harness) — coro query results identical via the dispatched
  task; sync query unchanged; one-task-per-session assert never trips; TSan clean on concurrent sessions.
- d2: DEBUG force-yield through the session/pool — multi-session: while session A's coro query is parked,
  session B's task runs on the freed worker; A resumes pinned + result-invariant; OOM/arena TLS intact;
  TSan clean (this is the first real cross-task park).
- Shutdown-while-parked: drop a connection mid-park → no UAF/leak (ASan + the ~PullPlan guard).
