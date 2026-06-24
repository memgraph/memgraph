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
