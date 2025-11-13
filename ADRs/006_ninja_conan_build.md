# Using Ninja and Conan for Faster, More Efficient Builds

**Author**
Matt James (github.com/mattkjames7)

**Status**
PROPOSED

**Date**
2025-11-13

**Problem**

Memgraph’s existing build pipeline relies primarily on CMake with the default generator and ad-hoc dependency management. Full builds are slow, with long periods of minimal CPU utilization. This reduces developer productivity, slows iteration during feature development, and increases CI turnaround time. We need a build system that minimizes wasted compute time, maximizes parallelism, and ensures reproducible dependency management across platforms.

**Criteria**

- Must significantly reduce full and incremental build times, and improve parallelism and CPU utilization.
- Must ensure consistent dependency versions, reliable artifact caching, and simplified environment setup.
- Must integrate cleanly into the current C++/CMake ecosystem, with low ongoing maintenance burden.

**Decision**

We adopt **Ninja** as the primary build generator and **Conan** as the dependency/package manager for Memgraph.

The rationale for this decision is as follows:

**Ninja** provides dramatically faster build orchestration using a highly optimized dependency graph evaluation and better parallelization across cores.
**Conan** provides a dependency management layer with consistent dependency versions, reliable artifact caching, and simplified environment setup.

Positive consequences include faster builds, more predictable CI performance, and reproducible dependency environments.
A negative consequence is that adding new dependencies may require creating a Conan recipe, which can be time-consuming.
