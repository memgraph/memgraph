# build_profile

Where does a memgraph build spend memory, CPU and wall time, step by step?
Ninja's own log only has timings, so this wraps every compile, link and custom
command in a tiny launcher that records the step's peak RSS and rusage, while a
second process samples the whole machine's memory. The result says which
translation units and links are the heavy ones, how much the build needed at its
worst moment, and whether the job-pool budgets in `cmake/BuildParallelism.cmake`
still match reality.

## Usage

```bash
# Full clean build, ccache disabled so every step really compiles
tools/build_profile/profile.sh

# Anything after the tool's own options goes to build.sh
tools/build_profile/profile.sh --build-type RelWithDebInfo --dev -DMG_ENABLE_TESTING=OFF

# Just re-render an earlier run
tools/build_profile/profile.sh --report-only --out build_profile_results/20260915_120000
```

### Inside the mgbuild container

`release/package/mgbuild.sh ... build-memgraph --profile` runs the normal
container build with the profiler attached and copies the results directory to
`build_profile_results/` on the host. The build itself is identical apart from
ccache being disabled for the run.

### In CI

The Diff workflow has a `profile_build` input (workflow_dispatch and
workflow_call). When set, every `build-memgraph` step in the Community,
Coverage, Debug, Jepsen and Release jobs runs with `--profile`, and each job
uploads its results directory as a `build_profile_<Workflow>-<job>` artifact,
even if the build failed. Builds run without ccache while profiling.

### Wrapping your own build command

If configure and build are separate steps in your flow (as they are in mgbuild),
arm the hook at configure time and let `--exec` wrap the build:

```bash
export MG_BUILD_PROFILE_LOG=$PWD/out/steps.jsonl
cmake --preset conan-release -DCMAKE_PROJECT_INCLUDE=$PWD/tools/build_profile/launcher.cmake
tools/build_profile/profile.sh --exec --out out -- cmake --build --preset conan-release
```

Results land in `build_profile_results/<timestamp>/`:

| file            | contents                                                            |
|-----------------|---------------------------------------------------------------------|
| `report.txt`    | the human-readable summary (also printed)                           |
| `plots.html`    | self-contained interactive charts: memory vs. build time, running steps, a zoomable step timeline coloured by kind or memory, and peak-vs-wall per step |
| `steps.jsonl`   | one JSON record per launched command, raw                           |
| `steps.csv`     | the same after folding multi-command links, one row per step        |
| `memory_timeline.csv` | per sample: machine used/available memory, cgroup usage, and the summed anon/RSS peaks and counts of the steps running at that moment |
| `sysmon.jsonl`  | machine-wide memory, CPU, load and memory-pressure samples          |
| `summary.json`  | machine-readable version of the report's headline numbers           |
| `trace.json`    | Chrome trace: steps on a timeline with memory counters; open it at https://ui.perfetto.dev |
| `build.log`     | build.sh output                                                     |

## How it works

* `launcher.cmake` is passed to CMake as `CMAKE_PROJECT_INCLUDE`, so the build
  files are not modified. It sets the `RULE_LAUNCH_COMPILE/LINK/CUSTOM` global
  properties, which make Ninja prefix each command with `step.py`. It only arms
  itself while `MG_BUILD_PROFILE_LOG` is in the environment, and the driver
  removes the cache entry afterwards with `cmake_cache.py`.
* `step.py` forks the real command, `wait4()`s it for rusage (peak RSS of the
  largest process in the tree, CPU time, faults, I/O) and meanwhile samples the
  summed RSS of the whole process tree through `/proc/*/task/*/children`, which
  is what catches a compiler driver plus its `cc1` child or cargo's `rustc`
  fan-out. Each record is appended as one JSON line. ccache still sits between
  the wrapper and the compiler; with `CCACHE_DISABLE=1` it just execs the compiler.
* `meta.py` writes the first line of `steps.jsonl`: host, commit, build.sh
  arguments and ccache mode, so a copied log still describes its run.
* `sysmon.py` samples `/proc/meminfo`, `/proc/stat`, `/proc/loadavg`,
  `/proc/pressure/memory` and the cgroup memory counter, so the report can put
  the per-step numbers next to what the machine actually experienced.
* `plots.py` fills `plots_template.html` with the same data; the page draws
  itself client-side with no libraries, so the file can be attached to a PR or
  CI artifact as is. Chart changes go in the template, data changes in the script.
* `report.py` merges and ranks. "peak" for a step is the larger of rusage
  `ru_maxrss` and the sampled tree peak; "anon" is the tree's resident minus
  file-backed pages, the part that is really the step's own and the number to
  size job pools by, since file-backed pages (mmapped archives, the compiler
  binary) are shared between concurrent steps and reclaimable. The
  "Memory over time" section puts what the machine used above its pre-build
  baseline next to the summed peaks of the steps running at that moment; the
  sum is an upper bound because steps do not all peak at the same instant.

## Caveats

* The launcher changes every Ninja command line, so a profiled build and a
  normal build do not share incremental state; expect a full rebuild when
  switching between them (cheap with ccache).
* With `--ccache`, a cache hit records ccache's footprint, not the compiler's.
  The report flags this.
* Steps shorter than the sampling interval only get the rusage number, which is
  exact for a single process and a slight underestimate for a short-lived tree.
* Ninja's own scheduling is untouched; the job pools cap concurrency exactly as
  in a normal build, so the "worst-case demand" figure describes this
  machine's configuration.
