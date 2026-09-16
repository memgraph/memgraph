# Reproducing slow-CI-runner behaviour locally

The self-hosted fleet splits into two throughput classes that `nproc` does not
distinguish, so `-j$(nproc)` overcommits half of it and tests cross ctest
timeouts calibrated on the fast half. These scripts reproduce that locally and
identify which resource actually runs out.

## Result

The device, not the CPU. Same 5 CPUs and the same `ctest -j5` throughout; the
only difference in the last column is a competing `dd` load using O_DIRECT on
the same NVMe:

| test | 5 distinct cores | 5 CPUs on 3 cores (SMT) | + competing IO |
|---|---|---|---|
| `storage_v2_indices` | 15.0s | 15.4s | 221.6s |
| `query_expression_evaluator` | 32.7s | 32.5s | 461.1s |
| `interpreter` | 27.7s | 24.3s | 383.8s |
| `storage_v2_constraints` | 25.9s | 23.9s | 216.5s |
| `utils_scheduler` (control) | 56.8s | 57.6s | 56.2s |

Packing the same number of logical CPUs onto SMT siblings changes the parallel
run by less than measurement noise. Competing IO slows every device-touching
test by 8-15x while leaving the sleep-bound control at 0.99x, which is the shape
the CI logs show.

`interpreter` and `query_expression_evaluator` are not CPU-bound despite their
names; both are slowed 10-14x by device contention. Classify tests by measuring
them, not by reading their names.

## Usage

Needs the unit test targets built (`cmake --build --preset conan-relwithdebinfo
--target memgraph__unit__...`). Then run repeatedly until it reports no steps
left:

    tools/ci/runner-contention/step.sh      # one ctest pass per invocation
    tools/ci/runner-contention/analyse.py   # per-class medians per arm

For the IO arm, start the competing load first and stop it afterwards:

    tools/ci/runner-contention/io_load.sh

`reclaim.py` frees clean page cache without root, for when accumulated build
cache has pinned `MemFree` near zero.

## Traps

Each of these yields a confident wrong answer rather than an error:

- `pgrep -x` matches the kernel's `comm`, capped at 15 characters, so long test
  names never match and any guard built on it always reads "clean".
- `pgrep -c` prints `0` *and* exits non-zero when nothing matches, so
  `$(pgrep -c x || echo 0)` yields `"0\n0"` and breaks the arithmetic.
- A first pass over a cold page cache runs several times slower, inflating the
  isolated baseline and shrinking every ratio derived from it. Hence the `warm`
  step.
- Many concurrent `ctest` processes against one build tree serialise on ctest's
  own state, which swamps the resource signal and makes every arm look
  identical. Use a single `ctest -j N`, which is what CI runs anyway.

Repeat the baseline arm last. That control is what caught two of the above.
