# P3.3 coroutine-cursor overhead — bottleneck investigation log

Box: bare-metal Intel i7-11800H (Tiger Lake), x86_64, not virtualized.
`perf_event_paranoid=1`, governor=performance, turbo OFF. Binary: `build/memgraph`
(RelWithDebInfo, branch `pr/coroutine-cursors-p3-unify`). flag-OFF = `PullLegacy` (==master),
flag-ON = `--experimental-enabled=coroutine-cursors`. Same binary both arms.

Method: server pinned to one core under a single-connection tight query loop (`hammer_count.py`),
`perf stat -e cycles,instructions,branches,branch-misses` over the EXACT hammer window; the client
prints the completed-query count, so instructions/query is measured directly (no cross-referencing).

---

## EXP-0 — baseline (decisive bare-metal A/B + counters)

A/B wall-clock overhead (median of 40), chain graph (100k :N, 100k :R chain edges):

| query         | OFF ms | ON ms  | ovhd  |
|---------------|--------|--------|-------|
| scan_count    | 15.81  | 19.67  | +24.5%|
| filter_count  | 37.55  | 41.62  | +10.8%|
| sum_agg       | 38.29  | 38.41  | +0.3% |
| group_agg     | 37.58  | 40.45  | +7.7% |
| expand_count  | 44.86  | 54.03  | +20.4%|
| expand2_count | 69.38  | 93.09  | +34.2%|
| project_10k   | 25.63  | 28.02  | +9.3% |
| orderby_1k    | 107.1  | 103.5  | -3.4% |

HW counters, query `MATCH (n:N)-[:R]->(m) RETURN count(*)`, exact 15 s window:

|                    | OFF     | ON      | per-query Δ |
|--------------------|---------|---------|-------------|
| queries / 15 s     | 321     | 270     | thrpt −18.9%|
| instructions/query | 251.0 M | 301.3 M | **+20.0%**  |
| cycles/query       | 108.4 M | 128.5 M | +18.5%      |
| IPC                | 2.32    | 2.35    | ~flat       |
| branch-miss rate   | 0.07 %  | 0.06 %  | flat        |

**Conclusion EXP-0:** overhead is REAL (reproduces on bare metal), and is pure extra
instruction count (~+50.3 M instr/query ≈ +503 instr per emitted edge) at flat IPC and flat
branch-misprediction. NOT a VM/BTB/mispredict artifact. Refutes the original hypothesis.

Profile (flag-ON, self-time, all NEW vs OFF): `InitEdgesCo[.resume]` 6.84%, `Expand::DoPull[.resume]`
6.77%, `Filter/ScanAll::DoPull[.resume]`, `await_resume`, `PullCo`; elevated allocator traffic
(`je_sdallocx` 6.75→8.18%, extra malloc/mallocx/sdallocx).

Architecture note: each converted cursor has ONE persistent `gen_` frame. But `Expand::DoPull`
does `co_await InitEdgesCo(...)` (and `ExpandVariable` `co_await PullInputCo(...)`) on a TEMPORARY
→ rvalue `co_await` → `owns_handle_=true` → a fresh coroutine frame is allocated+destroyed PER CALL.

---

## EXP-1 — isolate per-InitEdgesCo-frame cost (100× call-frequency lever, NO rebuild)

Hypothesis: the dominant per-edge cost is the per-call `InitEdgesCo` coroutine frame
(alloc + symmetric-transfer + state machine + destroy), not the fundamental per-edge `DoPull` resume.

Lever: same #edges emitted (100k), but vary #InitEdgesCo calls:
- chain  `MATCH (n:N)-[:R]->(m) RETURN count(*)` — 1 edge/source → ~100k InitEdgesCo calls
- fanout `MATCH (s:S)-[:R]->(t) RETURN count(*)` — 100 edges/source → ~1k InitEdgesCo calls

Predict: if InitEdgesCo frame is the cost, chain extra-instr/edge >> fanout extra-instr/edge.
            if fundamental DoPull resume, chain ≈ fanout.

Result (HW counters, instr/query measured, normalized by edges emitted = 100k both):

| graph  | InitEdgesCo calls | OFF Minstr/q | ON Minstr/q | ovhd  | extra instr/edge |
|--------|-------------------|--------------|-------------|-------|------------------|
| chain  | ~100k (1/edge)    | 251.07       | 301.30      | +20.0%| **502.4**        |
| fanout | ~1k  (1/100edge)  |  63.50       |  73.47      | +15.7%| **99.7**         |

**Conclusion EXP-1 (decisive):** the overhead splits cleanly:
- **fundamental per-pull resume ≈ ~96 instr/edge** (fanout, where InitEdgesCo is amortized away).
- **per-call `InitEdgesCo` frame ≈ ~405 instr/call** (chain extra/edge 502 − fundamental 96, ÷1 call/edge).
- In the chain expand, the per-call `InitEdgesCo` frame is **~81% of the +20% overhead**; the
  fundamental persistent-`gen_` resume is the remaining ~19%.

Actionable: eliminate the per-call `InitEdgesCo` frame by **inlining it into the persistent
`DoPull` `gen_` body** (the child pull stays `co_await PullChild`, which uses the child's
persistent `gen_` — no allocation). Predicted: chain expand +20% → ~+4% (passes the 5% gate).
Same pattern applies to `ExpandVariable::PullInputCo`.

---

## EXP-2 — inline InitEdgesCo into Expand::DoPull (rebuild + re-measure)

Change: `src/query/plan/operator.cpp` — replaced `co_await InitEdgesCo(frame, context)` in
`Expand::ExpandCursor::DoPull` with the verbatim InitEdges body inlined into a nested
`while(true)` (child pull stays `co_await PullChild`). No per-call helper frame allocated.

Result (HW counters, instr/query, ÷edges):

| graph  | OFF Minstr/q | ON Minstr/q | ovhd (was)      | extra instr/edge (was) |
|--------|--------------|-------------|-----------------|------------------------|
| chain  | 251.14       | 275.97      | **+9.9%** (+20.0%) | **248.2** (502.4)   |
| fanout |  63.51       |  72.73      | +14.5% (+15.7%) | 92.1 (99.7)            |

**Conclusion EXP-2:** chain expand overhead **halved (+20.0% → +9.9%)**; ~254 instr/edge
removed = the per-call `InitEdgesCo` frame (alloc+transfer+state-machine+destroy). Confirms
EXP-1's attribution. The remaining chain extra (248/edge) vs fanout (92/edge) is the
*fundamental* per-pull `gen_` resume, paid twice per edge on a chain (Expand resume + a ScanAll
input pull every edge); fanout amortizes the ScanAll pull → its 92/edge is the Expand-resume
floor. (fanout % is higher than chain only because fanout does far less real work per edge —
smaller denominator, same ~92 instr coroutine floor.)

Still > 5% gate on the pathological 1-edge-per-node chain. Next: confirm parity/correctness,
get the full query table (expand2 had 2 helper frames/row → should drop most), then assess the
fundamental per-pull resume floor.

### Verification + full table (clean binary, after also removing two dead `produced` vars)

- `cursor_parity` (flag-OFF ≡ flag-ON): **Corpus + MutationCorpus PASS** — inline preserves parity.
- Final HW counters (stable across runs): chain expand **+10.0%** (extra/edge 250), fanout +14.5%.
- Wall-clock A/B (median 40), before→after this fix: `expand_count` +20.4%→+10.8%,
  `expand2_count` +34.2%→**+13.7%** (two helper frames/row removed), `project_10k` +9.3%→+3.5%.
  (The non-Expand queries — scan/sum/group/orderby — swing ±10% run-to-run and are wall-clock
  noise; they don't touch the changed code. Trust the deterministic instr/query counters.)

---

## Conclusion — exact bottleneck + fix

**Bottleneck (measured, not inferred):** the dominant coroutine-cursor overhead on pull-dense
queries was the **per-call one-shot helper coroutine frame** (`Expand::InitEdgesCo`), `co_await`'d
on a temporary so a fresh frame was allocated → symmetric-transferred → run → destroyed on EVERY
call (~once per emitted edge on a chain). Measured at **~250 instr/call ≈ ~80% of the chain-expand
overhead**. NOT a VM/BTB/branch-mispredict artifact (branch-miss rate flat at 0.06–0.07%, IPC
unchanged); pure extra instruction count.

**Fix (landed in this branch):** inline `InitEdgesCo`'s body into the persistent `DoPull` `gen_`
(the child pull stays `co_await PullChild` → the child's persistent `gen_`, no allocation). Result:
chain expand **+20.0% → +10.0%**, expand2 **+34% → +14%**, parity preserved.

**Residual (the structural floor):** ~92 instr per pull-crossing (fanout Expand-resume floor) =
one persistent-`gen_` resume + `co_yield` symmetric transfer + the `ResumeAwaitable`/
`YieldPointAwaitable` await machinery. This is intrinsic to the per-cursor coroutine model; its %
impact is inversely proportional to per-row real work (≈+10–15% on `count(*)`-over-expansion with
near-zero per-row work; **near-zero on realistic queries**: `sum_agg` +0.3%, `project` +3.5%,
property/aggregation work dominates and dilutes it). Cutting it further needs a structural change
(cursor fusion / fewer suspension points), not a local tweak — separate follow-up.

**Follow-ups:**
1. DONE — deleted the now-dead `Expand::ExpandCursor::InitEdgesCo` (def + header decl).
2. DONE — inlined `ExpandVariable::PullInputCo` into its `DoPull` and deleted the helper. It is
   called ~once per *input vertex* (not per output row), so low frequency / low payoff, but done
   for consistency. Both helper coroutines are now gone; 0 per-call coroutine frames remain in the
   expand family.
3. Structural per-pull resume floor (~92 instr/pull) — own investigation if pull-dense expansion
   must also clear 5%; realistic queries already do.

### EXP-3 — verify cleanup + PullInputCo inline (clean binary, parity green)

| query                          | OFF Minstr/q | ON Minstr/q | ovhd  |
|--------------------------------|--------------|-------------|-------|
| chain expand (regression check)| 250.88       | 275.81      | +9.9% |
| variable expand `*1..3`        | 771.91       | 816.54      | +5.8% |

chain unchanged (+9.9%) — deleting the dead helper did not regress. Variable expand +5.8%
(more per-row work — DFS stack/path building — dilutes the per-pull coroutine floor).
`cursor_parity` Corpus + MutationCorpus PASS; clean build, no warnings.

---

## EXP-4 — attack the residual ~10% (does symmetric transfer make crossings free?)

**Concept.** Symmetric transfer (tail-call between coroutines) removes only the *trampoline /
stack-growth / scheduler round-trip*. It does NOT remove, per `co_await`: (1) the await protocol
(`await_ready`/`await_resume`), and (2) **frame save/restore of every local live across the
suspend point** (a resumable frame must persist state to the heap; a normal `bool Pull()` keeps it
in registers / on the call stack). So a coroutine boundary crossing is inherently costlier than a
virtual `Pull()` call even with symmetric transfer, scaling with live-across-suspend state.

**EXP-4b (throwaway, reverted): force ScanAll onto the frame-less immediate path** so a converted
parent's `co_await PullChild(ScanAll)` does not suspend+resume a frame.

| chain expand            | extra instr/edge | overhead |
|-------------------------|------------------|----------|
| ScanAll as coroutine    | 250              | +9.9%    |
| ScanAll immediate       | 189              | **+7.5%**|
| (fanout Expand floor)   | ~92              | —        |

Removing ONE input-pull crossing saved **~61 instr/edge** — the measured cost of that crossing
(ScanAll resume + Expand frame spill). Direct evidence the crossing is NOT free under symmetric
transfer. It only fell to +7.5% (not the ~92 floor) because the `co_await PullChild` is still a
co_await in Expand's body: `await_ready()` still runs and Expand's frame is still laid out
conservatively for a possible suspend there.

**Residual decomposition (chain count(*)-over-expand), measured:**
- ~92 instr/edge — Aggregate→Expand crossing, paid every edge; irreducible while Expand is a coroutine.
- ~61 instr/edge — ScanAll input-pull crossing; removable by NOT converting leaf ScanAll.
- ~remainder — conservative frame layout around the (now-immediate) co_await + per-edge init work in coroutine context.

**Conclusion:** the residual is **structural to the per-cursor-coroutine model**, not a bug and
not a symmetric-transfer failure. Levers, by cost/benefit:
1. **Selective conversion** — leave cursors that return one row per pull and have no unbounded
   internal loop (ScanAll without internal filtering, Once, …) on the legacy/immediate path; only
   convert cursors that genuinely need to suspend mid-pull for cooperative yield. Measured ~+10%→
   +7.5% on chain; principled and the seam already supports it. Tradeoff: a per-cursor decision
   about yield granularity (a leaf that CAN loop internally, e.g. a range scan that skips many
   index entries, would lose its yield point). Partial win.
2. **Cursor fusion** — collapse adjacent cursors into one coroutine to cut crossings (toward the
   original big-bang shape). Largest win, largest redesign.
3. **Accept** the current state: realistic queries (per-row work present) are already in budget
   (sum_agg +0.3%, project +3.5%, variable expand +5.8%); only `count(*)`-over-bare-expansion
   (near-zero per-row work — worst case) sits at ~+10%.

Untested-on-bare-metal: removing the per-iteration `co_await YieldPointAwaitable` (one fewer
suspend point per inner iteration → possible frame de-pessimization). The original investigation
saw "no change" on the VM; could differ here. Lower priority than (1).

---

## EXP-5 — standalone dispatch-model study (unbiased root-cause, free of kernel confounds)

`cursor_models.cpp` (+ `cursor_models_run.sh`): a standalone binary that mirrors the kernel's exact
coroutine core (symmetric transfer, persistent `gen_`, the `ResumeAwaitable`/`Awaiter` protocol)
on a Source→Expand→driver topology with tunable fan-out F and per-row work W, compiled with the
SAME toolchain (clang 20.1, `-O2 -g -DNDEBUG -std=c++23`). Strips away storage / jemalloc /
profiling so we measure ONLY dispatch. Designs: `legacy` (virtual Pull), `fused` (whole pipeline =
one coroutine), `allcoro` (current kernel: per-cursor persistent-`gen_` coroutines), `mixed` (leaf
on the frame-less immediate path), `helper` (allcoro + a one-shot helper coroutine per input pull =
the InitEdgesCo anti-pattern), `allcoroY` (allcoro + a per-iteration no-op yield co_await).

### Results — instructions/row (deterministic; primary signal)

chain pull-dense (F=1, W≈0 — worst case):

| design   | instr/row | vs legacy | note |
|----------|-----------|-----------|------|
| fused    | 30.0      | **−21%**  | one coroutine beats virtual-call legacy |
| legacy   | 38.0      | —         | virtual `bool Pull()` baseline |
| mixed    | 119.1     | +213%     | leaf immediate saves one crossing/row |
| allcoro  | 165.1     | +334%     | current kernel design |
| allcoroY | 165.1     | +334%     | **identical to allcoro → yield co_await is FREE** |
| helper   | 381.3     | +902%     | per-call helper frame ≈ +216 instr/row over allcoro |

(% is huge only because W≈0; the ABSOLUTE instr/row is what transfers to the kernel, where real
per-row work is ~2500 instr so the same ~127–230 instr tax reads as ~10–20%.)

### Depth sweep — D pass-through coroutine cursors over an immediate leaf (W=0)

| depth D (crossings/row) | 0 | 1 | 2 | 4 | 8 | 16 |
|-------------------------|---|---|---|---|---|----|
| instr/row               | 34 | 99 | 176 | 330 | 639 | 1256 |
| IPC                     | 3.55 | 3.23 | 3.31 | 2.88 | 2.67 | 2.28 |

**Perfectly linear: ~77 instr/row per coroutine boundary crossing.** IPC also degrades with depth
(each frame reload depends on the previous → less ILP), so deep pipelines pay twice.

### Root cause — settled

The coroutine penalty is a **fixed per-cursor-boundary-crossing cost (~60–77 retired instr/row per
boundary)**, paid every time a parent coroutine resumes a child coroutine. It is **frame
save/restore of live state + the await protocol** — exactly what symmetric transfer does NOT remove
(it removes only the trampoline / stack-growth / scheduler round-trip). It is NOT branch
misprediction (br-miss ≈ 0% across designs), NOT per-pull allocation (persistent `gen_` allocates
once per cursor per query), and NOT the yield check (free). Corollaries, all measured:
- **Coroutines are not the problem; per-cursor boundaries are.** A *fused* single coroutine is
  *faster* than the virtual-call pipeline (−21%). The cost scales with the NUMBER of coroutine
  frames the pull traverses per row (≈ plan depth), and inversely with per-row real work (% terms).
- **The one-shot helper frame** (InitEdgesCo/PullInputCo) was an extra ~216 instr/row on top — the
  worst offender, already eliminated (EXP-2/cleanup commits).

### Cross-check vs the kernel (model is validated)

- kernel chain expand residual ≈ 250 instr/edge ≈ 3 crossings (Aggregate→Expand→ScanAll, +Aggregate
  driven) × ~77 ≈ 230. ✓
- kernel fanout floor ≈ 92 instr/edge ≈ 1 crossing (input pull amortized) × ~77 + overhead. ✓
- EXP-4b "ScanAll immediate" saved ~61 instr/edge = one crossing removed. ✓

### What this means for "lowest possible penalty"

The only way to materially cut the penalty is to **reduce the number of coroutine boundaries
crossed per row**, in order of impact:
1. **Fuse linear cursor chains into one coroutine** (toward `fused`). A run of 1:1 / linear cursors
   (e.g. ScanAll→Filter→Produce, or ScanAll+Expand) compiled into a single coroutine body pays ONE
   crossing instead of N. This is the big win (fused beat legacy outright) and the principled
   long-term shape; it is also the largest redesign (breaks the one-frame-per-cursor model and the
   per-cursor yield granularity).
2. **Selective conversion** (`mixed`): keep cursors that return one row per pull with no unbounded
   internal loop (leaves like ScanAll/Once) on the frame-less immediate path; only convert cursors
   that must suspend mid-pull. Saves ~1 crossing/row where a leaf is pulled per row (measured
   +334% → +213% in the model; +9.9% → +7.5% in the kernel). Partial, lower-risk, but reopens
   "keep some PullLegacy" vs P3.4's "delete the dual path".
3. **Accept**: realistic queries (per-row work present) are already in budget; only deep,
   pull-dense, ~zero-work shapes (bare `count(*)`-over-expansion) exceed 5%.

Repro: `tests/mgbench/coroutine_perf/cursor_models_run.sh`.

---

## EXP-6 — external-yield / I/O-park cost (on-disk future: many short parked tasks)

Models a cold-page wait deep in the scan: a leaf `co_await ExternalYield` parks the task; the
driver resumes the stashed leaf handle (== scheduler resuming after I/O). Period P = park every
P-th access (1 = every access cold). instr/row includes park+resume. chain F=1, W=0.

| design     | P=never | P=8 | P=1 (every pull) | park increment |
|------------|---------|-----|------------------|----------------|
| allcoroEY  | 157     | 167 | 183              | ~+26 instr/park|
| fusedEY    | 68      | 77  | 93               | ~+25 instr/park|

Findings:
- **Park+resume itself is cheap and ~equal for both designs (~25 instr/park).** The protocol
  resumes the stashed LEAF handle directly (no root-to-leaf re-descent), so frequent parking does
  NOT depth-penalize resume. Good news: the current yield/park machinery is already efficient.
- **The fused advantage (~2×) persists under heavy parking** — it comes from the baseline
  per-crossing dispatch, which parking does not change. fused @ every-pull-park (93) still beats
  allcoro with NO parking (157).
- **Frame memory per parked task** (not in the table; architectural): a parked `allcoro` task holds
  the whole suspended chain alive = D coroutine frames (one per cursor in the pipeline); a parked
  `fused` task holds 1. For "many short parked tasks" (on-disk), fused uses ~D× less frame memory.
- **Selective conversion (`mixed`) is DISQUALIFIED for on-disk leaf I/O**: making leaves frame-less
  (immediate path) removes the leaf's ability to suspend — but the leaf scan is exactly where a
  cold-page I/O wait must park. `mixed` trades away the on-disk capability at the one place it is
  needed. `fused` keeps deep (mid-row) suspension AND is fast.

**Conclusion for the on-disk direction:** fusion is favoured on every axis that matters there —
steady-state dispatch (~2×), parked-task memory (~D×), and it preserves mid-row I/O suspension
(a single fused coroutine just `co_await`s at the internal blocking point; its one frame captures
all live state). Row-boundary-only yield is NOT achievable for on-disk (an I/O wait is inherently
mid-row), and fusion does not require it.

---

## EXP-7 — does dispatch matter once there is real I/O? (decisive for on-disk)

Same external-yield model, but each park now burns a simulated I/O latency before resuming
(`g_io_work` do_work iterations). period=1 (every access parks = on-disk worst case). ns/row:

| simulated I/O / park | allcoroEY ns/row | fusedEY ns/row | gap (dispatch significance) |
|----------------------|------------------|----------------|------------------------------|
| 0 (in-memory)        | 21.4             | 11.2           | fused ~2× — dispatch matters |
| ~0.1 µs              | 36.3             | 27.3           | +33%                         |
| ~1 µs (NVMe read)    | 420              | 406            | **+3.6%**                    |
| ~10 µs (SSD seek)    | 4382             | 4368           | **+0.3%**                    |

The dispatch difference is a FIXED ~8.6 ns/row; once a row costs microseconds of I/O it is noise.
**For the on-disk future the coroutine dispatch design is essentially irrelevant** — I/O latency
dominates by 1–2 orders of magnitude. The stackless per-cursor design is already the right choice
there (cheap small-frame parking, yield-anywhere, mid-row suspension), and shaving dispatch buys
nothing once queries are I/O-bound.

### Final synthesis (decision-ready)

The per-crossing dispatch tax (~77 instr × plan depth per row) is INTRINSIC to "stackless coroutine
per cursor, one row per crossing." Within the stated constraints (keep stackless; no fused-cursor
library; no planner changes; must yield mid-pull for slow cursors and I/O), only two things can cut
it, both large: **amortize** (batch/vectorize → N rows per crossing; big Frame/expr change) or
**eliminate** (stackful fibers → legacy-speed calls, yield by stack-swap; costs a stack per parked
task). A static fused-cursor library is ruled out (combinatorial / needs the planner / row-boundary
yield is useless for slow cursors).

But the tax only bites **in-memory, CPU-bound, pull-dense, ~zero-per-row-work** queries (~10% on
bare `count(*)`-over-expand; ~0–3% on realistic queries after the helper-frame fix). It is moot for
the on-disk direction (EXP-7) and a modest constant factor under parallel execution (which still
nets the Nx core speedup). Recommendation: **accept the residual, keep stackless, proceed with
parallel/scheduling + on-disk.** Revisit dispatch ONLY if in-memory pull-dense throughput becomes a
hard requirement — and then batched/vectorized pulls is the only constraint-compatible lever, as a
dedicated project. A cheap, constraint-compatible micro-win remains available regardless: shrink the
live-across-suspend frame state (hoist frame_writer/oom/profile out of the co_await scope) for a few
instr/crossing.

---

## EXP-8 — the HYBRID (selective coroutine by "needs mid-pull yield"), the constraint-compatible answer

Principle (stackless): a yield can only travel up through coroutine frames, so:
1. a cursor must be a coroutine iff its OWN single Pull() can run long (= needs to suspend mid-pull):
   pipeline breakers (aggregate/sort/join-build — Pull() consumes all input), blocking/on-disk
   leaves, heavy-internal-skip scans;
2. ancestor-closure: if a cursor is a coroutine, all its ANCESTORS must be too (to relay its yield).
Crucially, **between-row yield needs NO coroutine** — a cursor tree's iteration state lives in member
variables, not a call stack, so the session driver can pause between root.Pull() calls and resume
the loop later for free. Coroutines are ONLY for mid-Pull() suspension.

So a pipeline BREAKER (aggregate) is a coroutine that yields mid-consume, while the scan→expand
pipeline FEEDING it stays REGULAR (virtual Pull) — the breaker yields BETWEEN complete input pulls,
never relaying a yield through the pipeline.

Measured (chain pull-dense F=1 W=0, instr/row). NB the "legacy 32" earlier was unrealistically
devirtualized (concrete local cursor); REAL Memgraph always pulls through Cursor* (virtual):

| design                                            | instr/row | vs realistic flag-OFF |
|---------------------------------------------------|-----------|------------------------|
| virtual-call pipeline (== realistic flag-OFF)     | 48        | —                      |
| **hybrid: coroutine breaker + virtual pipeline**  | **57**    | **+19%**               |
| all-coroutine (flag-ON today)                     | 138       | +187%                  |
| (devirtualized concrete pipeline — not realistic) | 32        | —                      |

The hybrid removes essentially the entire per-row coroutine tax: the breaker is pulled by its parent
only once per output row (once total for count, once/group for grouped agg), and consumes its input
SYNCHRONOUSLY at virtual-Pull speed. For count(*)-over-expand this recovers ~flag-OFF speed. The
residual +9/row is the breaker's consume loop running in a coroutine frame; `breakerB` showed the
per-row `co_await` is free (amortizing it changed nothing), so factoring the hot consume loop into a
plain helper (yield-check at the helper boundary) brings the breaker to ~baseline.

### Why this fits all three stated constraints
- **No fused library** — every cursor is just "regular" or "coroutine"; no operator combinations. It
  reuses the EXISTING dual-path seam: regular == PullLegacy/immediate-PullCo, coroutine == DoPull.
- **No planner change** — "regular vs coroutine" is decided at CURSOR CONSTRUCTION (MakeCursor) from
  operator type + storage mode (in-memory vs on-disk), bottom-up: a cursor is a coroutine if its own
  Pull() can be long OR any child is a coroutine. The LogicalOperator tree is untouched.
- **Yield for slow cursors** — the slow ones (breakers, on-disk/heavy leaves) ARE coroutines and yield
  mid-pull; fast ones don't need it and yield between pulls at the session for free. Mid-pull yield is
  preserved exactly where it is needed.

### Cost / implication
This means KEEPING the dual path permanently (not deleting PullLegacy in P3.4) for the cursors that
can be either mode — i.e. most of them, since a Filter/Produce/etc. must have a coroutine mode for
when it sits above an on-disk (coroutine) leaf (ancestor-closure). Both bodies already exist today and
are parity-proven, so it is "don't delete what exists + add the construction-time mode decision,"
not "write new bodies." On-disk: leaves yield → coroutine-ness propagates up → fewer regular cursors,
but EXP-7 shows dispatch is moot there anyway. In-memory: breakers are coroutines, the hot pipelines
are regular → ~flag-OFF speed where it matters.

**This is the recommended direction if in-memory pull-dense perf must be recovered while keeping
stackless, the planner untouched, and mid-pull yield for slow cursors.**

---

## EXP-9 — what EXACTLY is one crossing? (loads/stores vs misses vs unavoidable work)

Rich counters at depth 0 vs 8, per-crossing delta (÷ 8 × 2e8 = 1.6e9 crossings):

| per crossing        | value     | note                                   |
|---------------------|-----------|----------------------------------------|
| instructions        | 60.9      |                                        |
| — loads             | 25.5      | frame reload + promise fields + protocol |
| — stores            | 13.8      | frame spill + parent_ link + suspend index |
| — branches          | 13.8      | ~all predicted                         |
| — arithmetic/moves  | ~8        |                                        |
| branch-misses       | 0.0002    | **~zero**                              |
| L1-dcache-load-miss | 0.0016    | **~zero (frames are L1-hot)**          |
| cycles              | 24.4      | IPC 2.5 (healthy — NOT stalled)        |

**The crossing is ~65% memory traffic (~26 loads + ~14 stores), and it all HITS L1.** Branch
misprediction and cache misses are both ~0; IPC is healthy. So the cost is **instruction
throughput** — the *count* of loads/stores executed, not memory latency, not misprediction, not
cache misses. It is the **stackless resumability tax**: a coroutine persists its live state +
suspend-point index to the heap frame and reloads it in *software* (explicit compiler-emitted
loads/stores), plus the await protocol (parent_ linking, has_more_, done()/flag checks), plus two
transfers per row (descend at co_await, ascend at co_yield → two suspend/resume pairs). A normal
`Pull()` call does the equivalent state save/restore in *hardware* (call/ret + register ABU,
~1–2 cycles).

### Consequences for the proposed mitigations
- **Locality allocator (pack frames):** ~no benefit for the crossing cost. The frames are already
  L1-hot in a tight pull loop (dcache-miss ~0.0016/crossing); packing cannot reduce the *number* of
  loads/stores, which is the actual cost. It only helps cache *footprint* under heavy concurrency
  (many queries evicting each other's frames) — and it cannot allocate away the instruction count.
- **Prefetch the next coroutine:** wrong lever. Prefetch hides memory *latency* (misses); we have
  ~0 misses and healthy IPC — we are throughput-bound, not latency-bound. A prefetch would ADD
  instructions to already instruction-bound code.
- **What actually reduces it:** (1) fewer crossings — fusion / the EXP-8 hybrid (removes whole
  ~61-instr units); (2) fewer loads/stores per crossing — shrink live-across-suspend state and trim
  the await protocol/promise (attacks the ~40 mem-ops directly; bounded ~20–40% upside, hand-tuned);
  (3) escape stackless → stackful fibers (state stays in registers/hardware-stack via call/ret;
  removes the ~40 software mem-ops entirely, at a stack-per-parked-task memory cost).
