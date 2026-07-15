# Graph Versioning v1 — branch-read allocation hot-spot investigation (perf box)

Self-contained package to run on a **real perf box** (full hardware PMU + working
call-graph unwinding). The dev VM couldn't resolve allocation call-graphs
(perf dwarf recursed, LBR unsupported, jemalloc not built with `--enable-prof`),
so this hands the last mile to a capable machine.

## The end goal (one sentence)

**Find the exact allocation call sites that make a branch query perform ~6–8× more
`malloc`/`free` than the identical query on `main`, so we can eliminate them.**

## What we already know (established on the VM by self-time profiling)

The Graph Versioning overlay model: a checked-out branch reads from `main`'s
frozen fork-state (`historical_`) unioned with a private `diff_engine`
(InMemoryStorage), gated by a per-object **"branched" bit** in a spare tag bit of
each Vertex/Edge's packed delta pointer. `branched()==false` ⇒ the object is
untouched by any branch ⇒ read `main` directly (fast path); `true` ⇒ reconcile
historical vs diff.

Measured, and **not** in dispute:

- **`branched()` is a lock-free relaxed-atomic load** — no re-locking to check the bit.
- **The versioning read path is ~free.** On a traversal-dominated deep-expansion,
  `BranchContext::ResolveEdges` = **0.21%**, `VertexAccessor::OutEdges` = 0.26%,
  `EdgeAccessor::To` = 0.42% self-time. The un-branched fast path IS being taken;
  we do **not** unify edges when the vertices aren't branched.
- **Real traversals are near-parity:** deep-expansion (thousands of vertices/query)
  is **1.13× branch/main**. Tiny shortest-path is **1.58×** only because it can't
  amortize per-query overhead.
- **The residual is ALLOCATION.** Normalized branch-vs-main self-time on the same
  deep-expansion: `je_malloc` ~6×, `je_sdallocx` ~6×, `operator delete` ~10×,
  `IsQueryTracked` ~11×, `EnsureJemallocThreadStateInitialized` ~7×
  (`IsQueryTracked` / `EnsureJemalloc…` are the per-allocation tracking hooks in
  `src/memory/malloc_free.cpp`, so they scale with allocation COUNT).

**So the branch does ~6–8× more allocations per query, but almost none of it is in
the versioning read code.** It's in the query execution / per-query setup path.
This script finds *where*.

## Hypotheses to distinguish

- **H1 — per-query diff-engine transaction setup.** Each branch query opens a
  transaction accessor on the private `diff_engine` in addition to the
  per-checkout historical pin (`CurrentDB::SetupDatabaseTransaction`,
  interpreter.cpp:220-284). If opening/tearing down that accessor allocates a lot,
  it's a FIXED per-query cost → dominates tiny queries (matches shortest-path
  1.58× vs deep 1.13×). **Signature:** the extra `je_malloc` callers sit under
  `Storage::Access` / transaction construction, NOT under the per-row operators;
  allocation count is ~flat per query regardless of rows.
- **H2 — per-row/per-vertex materialization.** The branch `VertexAccessor::OutEdges`
  fast path builds a `std::vector<query::EdgeAccessor>` per call. Main's does too,
  so this should be equal — but verify the branch isn't doing an EXTRA vector /
  copy (e.g. the `EdgeVertexAccessorResult` is moved on the branch arm but COPIED
  on main's, vertex_accessor.cpp — check which allocates more), or that the branch
  isn't forced off a batched/chunked path main uses. **Signature:** the extra
  `je_malloc` callers sit under `OutEdges` / `ExpandCursor` / `BuildResultOutEdges`
  and scale with row count.
- **H3 — memory-tracking / arena thrash from spanning two engines.** The branch
  touches two storages (main-historical + diff_engine); if allocations flip
  jemalloc thread/arena state between them, `EnsureJemallocThreadStateInitialized`
  fires repeatedly. **Signature:** `EnsureJemallocThreadStateInitialized` is a hot
  SELF frame (not just a passthrough) and its callers span both engines.

The **allocation caller tree** (branch vs main) distinguishes all three.

## How to run

Prereqs on the perf box: this branch checked out & built (RelWithDebInfo),
`python3` with the `neo4j` driver (`pip install neo4j`), `perf` usable
(`kernel.perf_event_paranoid <= 1`, or run perf as root).

```bash
# 1. (STRONGLY RECOMMENDED) build with frame pointers so call-graphs resolve cleanly.
#    Add to the CMake configure (RelWithDebInfo):
#      -DCMAKE_CXX_FLAGS="-fno-omit-frame-pointer" -DCMAKE_C_FLAGS="-fno-omit-frame-pointer"
#    then rebuild the `memgraph` target. (If your perf box unwinds dwarf well, you
#    can skip this and keep CG=dwarf — try it first; if the call trees look
#    self-recursive/garbage, do the FP rebuild and use CG=fp.)

# 2. run the investigation
cd perf-box
MG=/path/to/build/memgraph CG=dwarf EVENT=cycles CHURN=realistic ./run.sh
#   CG=fp        if you did the frame-pointer rebuild (best)
#   CHURN=worst  to also see the high-branched-density picture
#   EVENT=cpu-clock if the box has no hardware PMU after all
```

The script profiles four cells — `{main,branch} × {bfs (tiny), deep (big traversal)}` —
capturing for each: a flat self-time report, **per-allocation-symbol caller trees**,
and a `perf stat` hardware-counter summary.

## How to read the output (in `results-*/`)

1. **`*.alloc-callers.txt` — THE ANSWER.** For each allocator symbol (`je_malloc`,
   `operator new`, `newImpl`, …) it lists the CALLER frames. Compare
   `deep-branch.alloc-callers.txt` vs `deep-main.alloc-callers.txt`: the caller
   frame(s) that are **branch-only or much heavier on the branch** are the target.
   Map them to H1/H2/H3 by the signatures above.
2. **`*.flat.txt`** — confirms the self-time picture (versioning read path should be
   ~0.2–1%; allocation + tracking hooks should be the branch-heavy frames).
3. **`*.stat.txt`** — `cycles`, `instructions`, `cache-misses`, `LLC-load-misses`,
   `dTLB-load-misses` per cell. If branch has far more `instructions` (work) →
   allocation/CPU; if far more `cache/LLC misses` at similar instructions →
   pointer-chasing / locality (e.g. two-engine layout), a different fix.

## The most powerful alternative: jemalloc heap profiling

If you can rebuild jemalloc with **`--enable-prof`** (this build does NOT have it —
`JE_MALLOC_CONF=prof:true` returns `Invalid conf pair`), that's the cleanest
allocation attribution of all (jemalloc captures its own backtraces at alloc time,
independent of perf). Then:

```bash
JE_MALLOC_CONF="prof:true,prof_active:true,prof_final:true,prof_prefix:/tmp/jeprof,lg_prof_sample:10" \
  <run the branch driver to completion>          # dumps /tmp/jeprof.*.heap at clean exit
jeprof --show_bytes --text /path/to/build/memgraph /tmp/jeprof.*.heap | head -40
#   ^ ranks allocation sites by bytes with full stacks; run for main too and diff.
```
Note the env var is `JE_MALLOC_CONF` (this jemalloc is built with the `je_` prefix,
which renames `MALLOC_CONF`).

## What to send back

`results-*/` (the whole dir), or at minimum the four `*.alloc-callers.txt` and
`*.stat.txt`. That's enough to name the allocation site and design the fix.

## Files

- `run.sh` — the automation (also self-documents the exact perf commands).
- `driver.py` — stands up the branch (realistic/worst churn), then hammers one
  query (`bfs` = shortest-path, `deep` = 2-hop fan-out) in a tight loop; prints the
  memgraph PID to `--pidfile`.
- `bench.py` — the 10-run paired branch/main ratio harness (churn profiles, query
  set, dataset loader); `driver.py` reuses its setup functions.
- `pokec/` — pokec_small dataset (10k vertices / ~121k edges): index + import cypher.
- `docs/` — the investigation write-ups this package came out of (HTML): the final
  branch-vs-main perf report, the read-fast-path design, the Step-B materialized-edge
  design + skeptic verdict, and the earlier optimization investigations.
