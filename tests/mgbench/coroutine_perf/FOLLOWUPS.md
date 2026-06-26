# Coroutine cursors v2 — deferred follow-ups

Tracked, not-yet-implemented work for the coroutine split-point feature. Context:
[`PERF_GATE.md`](./PERF_GATE.md) (plan) + [`RESULTS.md`](./RESULTS.md) (bare-metal verdict).
The feature shipped with: the dual-path seam, the `--query-coroutine-yield-ops` knob (default =
the blocking operators `Aggregate,OrderBy,Accumulate,Distinct,HashJoin`; `All`/`*` = whole plan;
empty = kill switch), and split-policy observability (Prometheus counters + `[coro]` PROFILE
annotation).

---

## 1. Per-query split-point HINT (deferred — "not critical now")

**Goal.** Let a query override the global `--query-coroutine-yield-ops` knob for itself, so an
operator can tune the coroutine/synchronous split point on a hot query without changing the
instance-wide default.

**Proposed surface.** A pre-query directive, mirroring the existing `USING PARALLEL EXECUTION`:

```cypher
USING COROUTINE SPLIT Aggregate, OrderBy   MATCH ... RETURN ...   -- coro root->these ops, sync below
USING COROUTINE SPLIT ALL                  MATCH ... RETURN ...   -- whole plan coroutine
USING COROUTINE SPLIT NONE                 MATCH ... RETURN ...   -- fully synchronous (kill switch)
```

Open naming question: `USING COROUTINE SPLIT <ops>` vs `USING YIELD OPS <ops>`. Keep the op names
identical to the gflag tokens (`Aggregate, OrderBy, Accumulate, Distinct, HashJoin, All`) so the
knob and the hint share one vocabulary (`CoroOpFromName`).

**Plumbing (mirrors `USING PARALLEL EXECUTION`, which is the working template):**

1. **Grammar** — `src/query/frontend/opencypher/grammar/MemgraphCypher.g4`: add a
   `coroutineSplit` rule alongside `parallelExecution` in the pre-query directives list, e.g.
   `coroutineSplit : COROUTINE SPLIT ( ALL | NONE | symbolicName ( ',' symbolicName )* ) ;`
2. **AST** — `cypher_main_visitor.cpp` (`pre_query_directives_`, see the `parallel_execution_`
   handling at ~line 553): collect the listed op names into a new
   `pre_query_directives_.coroutine_split_ops_` (a `std::optional<std::vector<std::string>>`;
   `nullopt` = "use the gflag default", empty/`NONE` = disable, `ALL` = whole plan).
3. **Policy build** — `interpreter.cpp`: `CoroSplitPolicyFromFlags()` currently reads only
   `FLAGS_query_coroutine_yield_ops`. Add a `CoroSplitPolicyFrom(directive_ops)` overload (reuse
   the same comma/`All`/`*` parsing + `CoroOpFromName`) and have `MakeRootCursorWithPolicy` prefer
   the per-query directive when present, else fall back to the gflag. The thread-local
   `ActiveCoroPolicy()` mechanism is unchanged — only its source value changes.
4. **Profiling/observability** — no change needed; the existing `[coro]` PROFILE annotation and the
   `query_coroutine_driven` / `coroutine_region_cursors` counters already reflect whatever policy was
   active at `MakeCursor`, so they automatically reflect the hint.

**Tests.** Extend `tests/unit/cursor_knob.cpp`: a `USING COROUTINE SPLIT …` query produces results
identical to the same query under the matching gflag value, and `SPLIT NONE` forces a fully sync
plan (region tally == 0) even with the gflag default on. The `CoroSelectedCount()` tally + the
`[coro]` PROFILE check are the ready-made non-vacuity probes.

**Scope note.** Pure additive — no change to the seam, the drive, or the parallel cursors. It only
swaps where the active `CoroSplitPolicy` comes from for one query.

---

## 2. Re-validate the broadened default on the perf box

The bare-metal gate ([`RESULTS.md`](./RESULTS.md)) validated only the `Aggregate,OrderBy` subset.
The shipped default was later broadened to all blocking operators
(`Aggregate,OrderBy,Accumulate,Distinct,HashJoin`), which **enlarges the coroutine region** in plan
shapes that contain the new ops (the region now reaches down to the lowest blocking operator).

**To do on the next perf pass:**
- Re-run `run_ab.sh` / `perf_pmu.sh` (PERF_GATE §3.1–3.2) with the broadened default vs the
  `Aggregate,OrderBy` subset vs `All`, on plan shapes that exercise Accumulate / Distinct / HashJoin
  (e.g. `MATCH … RETURN DISTINCT …`, hash-joined patterns, write-then-RETURN with Accumulate).
- Confirm each new split op keeps SPLIT within the ~2% budget (hard ceiling 5%). If any regresses,
  drop it from the default via the gflag — no code change needed.
- Use the new metrics as the in-process signal: `coroutine_region_cursors_total /
  query_coroutine_driven_total` (average region size) should not balloon, and PROFILE `[coro]` should
  show the split landing at the lowest blocking operator, not creeping over a high-row subtree.

---

## 3. (Background) approach-B cooperative-yield scheduler

Out of scope for the throughput flip, but recorded: the yield-latency bench (RESULTS.md §5) showed
SPLIT preempts no better than root-only for heavy synchronous work **below** the split point. When
the self-park scheduler lands, revisit split-point/yield-point placement so heavy operators can be
preempted. Independent of the split-point set chosen for throughput here.
