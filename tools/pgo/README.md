# Profile-Guided Optimization (PGO) for Memgraph

PGO builds Memgraph twice: an **instrumented** binary that records which code runs
hottest while executing a representative workload, and a final binary the compiler
re-optimizes (inlining, block layout, hot/cold splitting) using that profile.

On forge (AMD Zen 4), instrumented PGO trained on pokec expansions gave, control-validated:
**~+25% throughput on short queries** and double-digit gains on traversals, and it
**generalized** to held-out query types (a never-trained `MATCH (n {id}) RETURN n` gained +31%).
It needs no PMU/LBR (unlike AutoFDO/BOLT), so it works anywhere the toolchain runs.

## Build knobs

Single `-D` token so it forwards cleanly through `build.sh` / `mgbuild.sh`:

| CMake | Meaning |
|---|---|
| `-DMG_PGO=generate` | instrument; raw profiles → `build/pgo-raw/` |
| `-DMG_PGO=use` | optimize using `build/pgo.profdata` (override with `-DMG_PGO_PROFILE=<file>`) |

Via `mgbuild.sh`:

```bash
# 1. instrumented build
mgbuild.sh ... build-memgraph --pgo generate
# 2. train (exercises the instrumented binary across the full mgbench query mix)
mgbuild.sh ... --enterprise-license "$L" --organization-name "$O" pgo-train --dataset pokec --size small
# 3. merge raw profiles -> build/pgo.profdata
mgbuild.sh ... pgo-merge
# 4. optimized build
mgbuild.sh ... build-memgraph --pgo use --split-debug
```

## The CI / Grafana experiment

`daily_benchmark.yaml` gained a `pgo` **workflow_dispatch** input. Trigger the job on a
branch (this one) with `pgo=true`; results upload to the bench-graph tagged by branch, so
its series overlays master's (un-PGO'd) series across the **whole** suite.

Keep the branch = master + this PGO build change **only**, so the branch-vs-master delta on
Grafana is *pure PGO effect* (isolates it — this is the correct use of PGO-in-benchmark, not a
code-regression gate). Do **not** enable `pgo` on the scheduled master run — keep master's
tracked series un-PGO'd for clean history.

```
gh workflow run daily_benchmark.yaml --ref experiment/pgo-benchmark -f pgo=true -f loop_count=10
```

For a **quick check** (the full daily suite is long), `benchmark_pgo.yaml` runs the same
PGO train/build but only the pokec-**medium** mgbench, uploaded tagged by branch. Trained the
same way (pokec/small, all groups).

**Run it without merging to master** — `workflow_dispatch` needs the file on the default
branch, so before the PR merges use the **label trigger**: add the `run-pgo-benchmark` label to
the PR (it runs the PR branch's workflow via `pull_request`). Re-add the label to re-run.

```
gh label create run-pgo-benchmark --color ededed        # once
gh api repos/memgraph/memgraph/issues/<PR>/labels -X POST -f "labels[]=run-pgo-benchmark"
```

After the PR is on master, the manual dispatch also works and takes inputs:

```
gh workflow run benchmark_pgo.yaml --ref experiment/pgo-benchmark -f pgo=true -f loop_count=3
```

On a label run PGO is on and `loop_count=3`; dispatch a `pgo=false` run if you want a
same-machine control alongside master's series.

## What to train on (the important part)

**Recommendation: train on the full mgbench pokec query mix — `pokec/small/*/*` (all groups).**
`pgo-train` does this by default. It's the "super general" set because it exercises the two
things PGO optimizes:

1. **Shared per-query machinery** — parse / query-strip / plan-cache lookup / operator `Pull`
   framework / Bolt encode / thread-pool dispatch. *Every* query runs this, so profiling it
   lifts essentially all queries (why a never-trained point-read still gained +31%).
2. **Common hot operators** — `ScanAll`, indexed lookup, `Expand`, `Filter`, `Produce`,
   `Distinct`, `Aggregate`. pokec's groups cover reads, writes, 1–4-hop expansions (+filters),
   and aggregations (count/min/max/avg) — the OLTP graph core.

**Coverage is what matters, not data size or query count.** Measured caveat: a hot loop *not*
exercised during training gets almost no benefit — expansion-only training left a full-scan
aggregate at +2%. So:

- Train on **all query groups** (`*/*`), not one family. ✅ (default)
- Use **pokec small** — the instrumented binary is 2–4× slower, and PGO cares about *code paths*
  (query shapes), not cache footprint. Small is fast and covers every shape. Bump to `medium`
  only if you specifically want more realistic memory behavior in the profile.
- If production runs query types pokec lacks (vector/text search, shortest-path, `load_parquet`,
  heavy analytics), add them to training so those hot loops get profiled. The most future-proof
  policy: **train on whatever the daily benchmark measures** — then the profile always matches
  what's tracked. `pgo-train` can be extended, or run extra `benchmark.py` globs into
  `build/pgo-raw/` before `pgo-merge`.

Don't over-engineer coverage — the broad pokec mix is the 80/20; expansions + aggregations +
reads/writes already generalize well.

## Caveats

- **Train/test overlap:** training on mgbench and then benchmarking mgbench is optimistic
  ("train on test"). It's the realistic case when production traffic ≈ benchmark traffic, and
  the shared-machinery win generalizes regardless — but read the absolute numbers with that in mind.
- **First CI run is a shakeout.** The profile plumbing (instrumented binary writing to
  `build/pgo-raw/` via `LLVM_PROFILE_FILE=...%p_%m.profraw`, then `llvm-profdata merge`) is
  wired but has not run in this CI; watch the "PGO — instrument, train, merge" step logs.
- **Not BOLT/AutoFDO.** Those need branch-record (LBR) sampling, unavailable on VMs; instrumented
  PGO is deliberate here and needs no PMU.
- **~2× build time** on the PGO path (two full builds + a training run). Fine for the nightly
  window; for production, generate the profile on a cadence and cache it rather than every build.
