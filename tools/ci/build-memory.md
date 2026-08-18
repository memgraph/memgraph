# Build memory and CI OOM

Why CI runs out of memory, what a build actually costs, and which knobs bound it.

## The shape of the problem

Peak memory during a build is affine in the number of concurrent jobs, while the
core count of a runner is fixed and its RAM is fixed independently. An uncapped
`-j$(nproc)` therefore asks for memory proportional to the *core* count on a
machine whose *memory* budget has nothing to do with it. A runner with a high
core-to-RAM ratio runs out of memory long before it runs out of cores.

Measured on a full cold Debug build (742 C++ translation units, 350 link steps,
ccache bypassed so every TU compiles), sampling `AnonPages` from
`/proc/meminfo` -- anonymous memory, because that is the part the kernel cannot
reclaim and therefore the part that drives the OOM killer:

| jobs | peak build memory | per job | wall clock |
|------|-------------------|---------|------------|
| 8    | 12.6 GB           | 1.58 GB | 13:27      |
| 22   | 27.8 GB           | 1.27 GB | 9:18       |
| 44   | 49.4 GB           | 1.12 GB | killed     |

Least squares over those points:

```
peak_GB = 4.81 + 1.019 * jobs
```

which reproduces all three within 0.5 GB. Extrapolating to runner sizes:

| cores | memory needed at `-j$(nproc)` |
|-------|-------------------------------|
| 32    | 37 GB                         |
| 64    | 70 GB                         |
| 96    | 103 GB                        |
| 128   | 135 GB                        |

The 44-job row above is not a projection. It exhausted a 62 GB machine,
drove `MemAvailable` to 0.7 GB and was killed part way through the build.

Note the wall clock column. Going from 8 jobs to 22 -- 2.75x the parallelism --
bought 31% less wall time. Compile parallelism is deep into diminishing returns,
so capping it costs far less time than the memory it saves.

Checked the other way round: given a 24 GB budget the formula below picks 13
jobs, and a full build at `-j13` under a hard 24 GB cgroup limit completed with
a peak of 19.8 GB. The model predicted 18.1 GB, so it runs about 9% optimistic
at that end of the range -- which is what the margin in the constants is for.

## Where the memory goes

Compiling is the expensive phase, not linking.

| | count | mean | p50 | p90 | p99 | max |
|---|-------|------|-----|-----|-----|-----|
| compile | 742 | 0.73 GB | 0.40 | 1.76 | 2.65 | 3.51 GB |
| link    | 350 | 1.36 GB | 0.42 | 4.04 | 4.12 | 4.45 GB |

Those are peak RSS, and for links RSS is misleading: a link step shows ~4.1 GB
resident but costs only ~0.8 GB of anonymous memory, because most of its
resident set is the shared, file-backed mapping of static libraries that every
concurrent link maps too. Capping links reclaims about half as much memory per
job forgone as capping compiles does.

The heaviest translation units are in the query layer -- `interpreter.cpp` at
3.51 GB, `plan/operator.cpp` at 3.43 GB, `cypher_query_interpreter.cpp` at
2.97 GB -- followed by the storage indices and durability units around 2 GB.
A single job can therefore need ~3.5 GB, which is why the per-thread budget
cannot safely be driven much below that when the job count is small.

## The three unbounded surfaces

A build spawns work from three schedulers that do not know about each other.
Capping one leaves the others free to exhaust the machine.

1. **Ninja**, via `cmake --build ... -j$(nproc)`. The dominant consumer, and the
   one the table above measures.
2. **Conan**, when a dependency is not in the cache. Each dependency's own build
   runs at `$(nproc)`; the install log shows `cmake --build ... -- -j22` per
   package. A toolchain bump changes `compiler.version` in the profile, which
   changes every `package_id`, which turns a routine build into a from-source
   rebuild of the entire graph -- arrow, aws-sdk-cpp, boost, rocksdb, protobuf,
   pulsar-client, librdkafka.
3. **Cargo**, for `mgcxx/text_search`. It picks its own rustc parallelism from
   inside a single Ninja edge, so it multiplies with Ninja's rather than
   sharing it.

## The knobs

`tools/ci/compute-build-threads.sh <gb_per_thread> [reserve_gb]` prints
`min(nproc, floor((MemAvailableGB - reserve_gb) / gb_per_thread))`. Splitting the
budget into a per-thread cost and a fixed reserve mirrors the affine model: the
reserve covers the intercept and whatever else the runner holds, so the
per-thread figure keeps meaning what it says and can be re-measured on its own.

`release/package/mgbuild.sh build-memgraph` applies it automatically. With no
flags it now derives its own limits rather than using `$(nproc)`:

- `--threads N` -- Ninja's global `-j`. Defaults to
  `compute-build-threads.sh 1.3 6`.
- `--dep-threads N` -- Conan's `tools.build:jobs` and cargo's
  `CARGO_BUILD_JOBS`. Defaults to `compute-build-threads.sh 2.0 6`.
- `--compile-threads N` / `--link-threads N` -- separate Ninja job pools, for
  when compiles and links want different limits. Off by default; the global
  `-j` is sized for compiles, which are the expensive class, so links are
  already covered.

Because the defaults live in `mgbuild.sh` rather than in each workflow, a new
workflow inherits them without having to remember to opt in.

The per-thread constants carry roughly 27% margin over the measured 1.02 GB
slope. `MEM_GB_PER_DEP_THREAD` is a prudent default rather than a measured one:
the mechanism is confirmed from Conan's build log, but the per-job cost of an
arrow or aws-sdk-cpp compile was not measured.

The limits are computed on the host rather than inside the build container,
which is what the container sees too as long as it runs without `--memory` and
`--cpus`. Give the containers explicit limits and the calculation has to move
inside them.

## A loose end

Several workflows pass `--link-threads $(compute-build-threads.sh 3.5)`
explicitly. That predates the measurements here and budgets links at 3.5 GB
each, roughly four times what a link actually costs in unreclaimable memory.
With the global `-j` now sized for compiles -- the more expensive class -- those
flags no longer protect against anything, and they throttle the link phase.
They can be dropped, which is a small wall-clock win rather than a correctness
fix, so it is left as a separate decision for whoever owns those workflows.

## Re-measuring

The numbers above are a property of the code, and the query and storage
translation units are the ones that move. Re-measure after a change that grows
them substantially, or after a toolchain bump:

1. Configure a Debug build with `CCACHE_PROGRAM` pointed at a wrapper that
   records `/usr/bin/time -f %M` per job, so every TU compiles cold. Point
   `CMAKE_<LANG>_LINKER_LAUNCHER` at the same wrapper for link steps.
2. Sample `AnonPages` from `/proc/meminfo` during a clean build, subtracting the
   pre-build baseline. Use anonymous memory, not RSS, and not `MemAvailable`.
3. Repeat at three job counts and fit a line. Set the per-thread constant above
   the slope, and the reserve above the intercept.
