# B2 shared core — interpreter-level resumable pull (VERIFICATION-CLEARED design)

Branch `feat/coroutine-scheduler-redesign` @ `053ff6c5c`. Shared core both Option A and Option B need
(Bolt-integration chosen later). Goal: make `PullPlan::Pull` **park mid-row / mid-batch** — suspend the
in-flight coroutine, return up to its caller WITHOUT freeing the coroutine, losing batch position, or
sending any protocol message — and on a later re-entry **resume** the suspended coroutine and finish the
batch with results byte-identical to a non-parked run. Verified single-threaded behind a DEBUG force-park
seam; the worker-freeing/HP/cross-worker resume is the A/B layer on top.

> **This revision incorporates the logic-verifier + skeptic + hard-logic-verifier findings (2026-06-30).**
> Changes vs the first draft are marked **[v]**. Verdict was "NOT implementable as first specced — 3 HIGH
> blockers"; those are resolved below.

## Ground truth (re-verified against the code)
- `cursor_->gen_` (a `PullAwaitable` member of the cursor, `cursor_awaitable_core.hpp:309-313`) **OWNS** the
  suspended coroutine frame. `PullCo` (the `MG_COROUTINE_CURSOR_PULLCO` macro, :320-331) does
  `if (!gen_) gen_ = DoPull(...); return gen_->Resume();`. `Resume()` returns a **non-owning**
  `ResumeAwaitable` (:252,307). The suspended child/leaf chain is owned transitively by the per-frame
  Awaiters (:185-192, "Awaiter MUST retain ownership of the child handle"; destroy is NOT recursive).
- **[v] ⇒ no `inflight_ra_` member.** `cursor_` is already a PullPlan member, so `gen_` (and the whole
  suspended chain) persists across a park for free. On resume, a fresh `cursor_->PullCo()` returns
  `gen_->Resume()` — a new non-owning view of the SAME suspended frame. The first draft's `inflight_ra_`
  was redundant (it does not own the frame). DROP IT.
- `driver_scope` (interpreter.cpp:3396) is a **LOCAL**; its `slot_` is where `ctx_.suspended_task_handle_ptr`
  points. Destroyed on every Pull return → the stash pointer dangles on resume. **MUST hoist to a member.**
- `int i` (interpreter.cpp:3431) is a LOCAL batch counter; lost on park-return. **MUST hoist.**
- `has_unsent_results_` is already a member (interpreter.cpp:3266). ✓
- `query_handler` is `std::move_only_function<std::optional<QueryHandlerResult>(AnyStream*, optional<int>)>`
  (interpreter.hpp:211). Today `nullopt`=BatchContinues (`has_more:true`), `Some(COMMIT/ABORT/NOTHING)`=done.
  `Interpreter::Pull` returns `std::map` UNCONDITIONALLY (interpreter.hpp:638) — NOT optional.
- S1's `Yielded`→`continue` re-resume (interpreter.cpp:3412) is a dormant no-op (nothing sets
  `yield_requested`). The core REPLACES it with park-return.

## Members to add to PullPlan **[v]** (declared AFTER `ctx_`, BEFORE `cursor_` — destruction order, see Lifetime)
```cpp
std::optional<plan::PullDriverScope> driver_scope_;  // hoisted; constructed once when coro-drive starts,
                                                     // destroyed when query Finishes or ~PullPlan; keeps
                                                     // slot_ alive so ctx_.suspended_task_handle_ptr is valid across park
int batch_i_{0};                  // hoisted batch position (S-2)
bool resuming_from_park_{false};  // set on park-return; on re-entry restore batch_i_ + skip fresh-batch preamble
bool shutdown_called_{false};     // [v] guards ~PullPlan cursor_->Shutdown() (MED fix)
memory::MemTracker::OOMCounter park_saved_oom_{};  // [v] OOM counter save across park (HIGH-2), see below
```
NO `inflight_ra_`.

## PullOutcome — the real tri-state **[v] (HIGH-1 fix; MINIMAL-RIPPLE approach)**
`nullopt` is already BatchContinues, so park needs a genuine 3rd value. **Verified: only ONE handler — the
Cypher pull at interpreter.cpp:3809 — drives the real coro `PullPlan::Pull`. The other ~20 handlers use
`PullPlanVector::Pull` (in-memory, returns bool, NEVER parks).** So make the change zero-ripple to those 20:
```cpp
enum class PullOutcome : uint8_t { Finished, BatchContinues, Parked };
// query_handler return type. Implicit ctors keep the other ~20 handlers UNEDITED.
struct PullResult {
  PullOutcome outcome{PullOutcome::BatchContinues};
  std::optional<QueryHandlerResult> result{};            // set iff Finished
  PullResult(QueryHandlerResult r) : outcome(PullOutcome::Finished), result(r) {}   // implicit: `return COMMIT;`
  PullResult(std::nullopt_t) {}                            // implicit: `return std::nullopt;` ⇒ BatchContinues
  static PullResult Parked() { return {PullOutcome::Parked, std::nullopt}; }
};
```
- `query_handler`'s return type: `std::optional<QueryHandlerResult>` → `PullResult`. The ~20 `PullPlanVector`
  handlers compile UNCHANGED (their `return QueryHandlerResult::COMMIT;` / `return std::nullopt;` hit the
  implicit ctors). Only the **3809** handler changes: `auto oc = pull_plan->Pull(...); switch(oc){ Finished→
  return <result>; Parked→return PullResult::Parked(); BatchContinues→return std::nullopt; }`.
- `PullPlan::Pull` returns `PullOutcome` (was `optional<ProfilingStatsWithTotalTime>`); stats handed back via
  an out-param/member on Finished (the 3809 handler already captures what it needs; the stats currently flow
  through the optional — re-home them to a PullPlan member read on Finished).
- `Interpreter::Pull` (interpreter.hpp:638-801) dispatch at line 687: switch on `maybe_res.outcome` —
  `Finished`→ current finished path (use `maybe_res.result`); `BatchContinues`→ `return {{"has_more",true}}`
  (unchanged); **`Parked`→ write NOTHING, no commit/reset; signal parked to caller via a new out-param**
  `bool *parked=nullptr` (`if (parked) *parked=true; return {};`). Caller (SessionHL::Pull / A/B task driver /
  the core test driver) re-invokes to resume on parked. (Out-param chosen over `optional<map>` return to
  minimize caller ripple — only callers that care pass `&parked`.)
- **c-core-1 = all of the above with `PullPlan::Pull` NEVER returning Parked yet** (drive unchanged) ⇒
  byte-identical behavior; the Parked branches are present but unreachable. c-core-2 flips the drive to
  produce Parked.

## PullPlan::Pull control flow **[v]**
```
PullOutcome Pull(stream, n, output_symbols, summary):
  StartTracking...                                    // unchanged (per-Pull; benign across park, Q5-confirmed)
  // [v] HIGH-3: accumulate execution time on EVERY exit incl. park, not only the normal tail.
  utils::Timer timer; auto add_time = OnScopeExit{[&]{ execution_time_ += timer.Elapsed(); }};
  if drive_coro && !driver_scope_:
      driver_scope_.emplace(ctx_, Enabled)            // ONCE per query (guarded by !driver_scope_)
      park_baseline_oom_ = MemTracker current OOM counter   // [v] HIGH-2 baseline at drive start
  #ifndef NDEBUG  set force-yield flag (idempotent; same pointer)  #endif
  if resuming_from_park_:                              // [v] HIGH-2: re-establish parked OOM counter
      restore MemTracker OOM counter = park_saved_oom_
  // do_pull drives one row; tri-state. The suspended frame lives in cursor_->gen_ (no local owns it).
  do_pull() -> Step{Row,Done,Parked}:
    if !drive_coro: return cursor_->Pull(frame_,ctx_) ? Row : Done
    ra = cursor_->PullCo(frame_, ctx_)                // fresh non-owning view of gen_ (resumes if suspended)
    r  = ResumePullStep(ra, ctx_)
    if r == Yielded:                                  // [v] stashed handle present ⇒ PARK (no re-resume)
        park_saved_oom_ = MemTracker current OOM counter      // [v] HIGH-2: save delta-in-parked-frame
        restore MemTracker OOM counter = park_baseline_oom_   // [v] clean the worker during park
        return Parked
    return r == HasRow ? Row : Done
  if !resuming_from_park_:
      batch_i_ = 0
      if has_unsent_results_ && !output_symbols.empty(): stream_values(); ++batch_i_
  resuming_from_park_ = false
  for (; !n || batch_i_ < n; ++batch_i_):
      step = do_pull()
      if step == Parked: resuming_from_park_ = true; return PullOutcome::Parked   // batch_i_ saved in member
      if step == Done: break
      if !output_symbols.empty(): stream_values()
  // end-of-batch probe (only when we streamed exactly n): single resuming_from_park_ flag is SUFFICIENT —
  // batch_i_==n implicitly distinguishes "in probe" from "in loop" (Q4 verified; the loop won't re-run on
  // resume because batch_i_==n already). Probe can also park:
  if batch_i_ == n:
      step = do_pull()
      if step == Parked: resuming_from_park_ = true; return PullOutcome::Parked   // re-enters here (loop skipped)
      has_unsent_results_ = (step == Row)
  else: has_unsent_results_ = false
  if has_unsent_results_: return PullOutcome::BatchContinues
  ...summary, cursor_->Shutdown() + shutdown_called_=true, stats...
  driver_scope_.reset()                               // drive done for this query
  return PullOutcome::Finished
```
NOTE the `add_time` OnScopeExit replaces the old `execution_time_ += timer.Elapsed()` at line 3454 (which a
park-return skipped). This also fixes `ctx_.profile_execution_time` (derived from `execution_time_`).

## Lifetime / destruction **[v] (Q1 fix)**
- Declare `driver_scope_` AFTER `ctx_` (its dtor writes `ctx_.suspended_task_handle_ptr`/`enabled_driver_active`
  back — must run before `ctx_` dies) and BEFORE `cursor_` (the suspended `gen_` chain in `cursor_` must
  outlive all scope teardown, then `~PullAwaitable` destroys the frames; destroy is non-recursive but the
  Awaiter chain handles children — :185-192).
- `~PullPlan`: if `cursor_ && !shutdown_called_`, call `cursor_->Shutdown()` (else `CallProcedure`'s cleanup
  lambda leaks when a query is destroyed while parked — client disconnect mid-park). The suspended
  coroutine frames are then freed by `~cursor_`/`~PullAwaitable`.

## OOMExceptionEnabler counter across park **[v] (HIGH-2)**
Every converted `DoPull` constructs `OOMExceptionEnabler` (RAII, `thread_local OutOfMemoryExceptionEnabler::counter_`)
BEFORE its `co_await YieldPointAwaitable`. At a park the enabler is alive on the suspended frame, so
`counter_` stays incremented on the worker thread → another query later running on that worker spuriously
observes `CanThrow()==true`. Per-cursor edits are insufficient (a parent's enabler can be live while a child
parks). Fix is at the driver boundary (this core), making the counter travel with the execution context:
- capture `park_baseline_oom_` when the drive scope starts (counter value with no in-flight enablers);
- on a park (do_pull → Parked): save `park_saved_oom_ = counter`, then `counter = park_baseline_oom_`
  (worker is clean during park);
- on resume (Pull re-entry with `resuming_from_park_`): `counter = park_saved_oom_` before the first
  `ResumePullStep`, so the coroutine resumes with its enablers "present" and their dtors balance it back.
- Composes with nested parks because the counter is a single scalar snapshotted at each park boundary.
- DORMANT in the single-threaded core (same-thread immediate resume keeps it balanced) but built here
  because the driver is its only correct home; the test asserts `counter == baseline` after a park-return
  (proves the save fired). HARD prerequisite for the A/B layer; do NOT defer the mechanism.
- VERIFY the exact MemTracker API for reading/writing the counter (it may need a small accessor;
  `OutOfMemoryExceptionEnabler` in `utils/memory_tracker.hpp` ~:129).

## What is NOT changed (verified sound)
- Q2 `driver_scope_` as query-lifetime member: `enabled_driver_active` is per-context (context.hpp:192-193),
  no false-trip while another query runs on the worker during park; PullPlan is heap-stable so `&slot_`
  doesn't move. Re-entry doesn't re-emplace (`!driver_scope_` guard).
- Q3 batch accounting: all six cases (park mid-loop / probe / has_unsent set / n=all / empty / single /
  Done-at-n) reproduce identical row order — verified.
- Q4 single `resuming_from_park_` flag sufficient (the first draft's "need sub-state" was a FALSE ALARM).
- Q7 profile path (`!is_profile_query`) never coro-drives, never parks — byte-identical.

## Staged implementation
- **c-core-1 (type plumbing, behavior-identical):** introduce `PullOutcome` + the `query_handler`/
  `PullPlan::Pull`/`Interpreter::Pull` return-type changes, with `Parked` NEVER produced yet (drive still
  re-resumes/▸ today). Build + full existing unit/e2e green; behavior byte-identical (no park).
- **c-core-2 (park behavior + fixes + test):** hoist `driver_scope_`/`batch_i_`/`resuming_from_park_`/
  `shutdown_called_`/OOM members (correct decl order); drive returns Parked on stashed handle; OOM
  save/restore; `add_time` OnScopeExit; `~PullPlan` shutdown guard; extend the DEBUG force-park seam;
  update `cursor_knob.cpp` `ForcedYieldDriveIsResultInvariant` to loop on the parked sentinel.

## Test (non-vacuous)
DEBUG force-park seam: set the throttle `ResettableCounter` to **1** (else small datasets fire zero parks
— vacuous) AND force `yield_requested`. A test driver loops `Interpreter::Pull` re-entering on the parked
sentinel until `Finished`, over the batched-PULL corpus (n=1,2,7,all) on read/write/aggregate/orderby
plans. Assert: (a) result rows+order+summary identical to the knob-off non-parked run; (b) the park path
fired N>0 times (probe counter — non-vacuous); (c) the n=7 batched case (S-2 over-stream trap) matches;
(d) **park-in-probe** with BOTH `has_unsent_results_=true` and `=false` entry states; (e) the OOM
`counter_ == baseline` immediately after a park-return.
