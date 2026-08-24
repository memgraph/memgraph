# Toolchain v9

v9 is the first toolchain produced by a restructured build. The LLVM and GCC
bumps come after that restructure, not with it.

That ordering is the whole plan: the current build is a 1700-line sequential
bash script with no caching, so every change costs a full rebuild. Bumping LLVM
on top of that means iterating on a multi-hour feedback loop. Restructuring
first makes the bump cheap, and doing them separately means a failure has one
candidate cause instead of two.

## What the toolchain build actually needs

Measured against v8 rather than assumed, because most of these contradicted the
first guess:

- **The tree already relocates.** GCC resolves its sysroot from `argv[0]`
  (`bin/../sysroot`), because `--with-sysroot` points inside `$PREFIX`. `gcc`
  and `g++` carry no RPATH; `clang`, `clang++` and `ld.lld` carry
  `[/opt/toolchain-v8/lib64:$ORIGIN/../lib]`, so they fall back correctly. From
  a copy at an unrelated path, both compilers built and linked working C++20
  binaries with no references back to the original prefix.
- **gdb is the one exception.** Its RUNPATH is
  `[/opt/toolchain-v8/sysroot/usr/lib:/opt/toolchain-v8/lib]` with no `$ORIGIN`
  entry, so from a moved tree it reaches back into the original prefix and dies
  on `GLIBC_2.38 not found`. That needs an `$ORIGIN`-relative rpath.
- **Stage outputs are already isolable.** Every sysroot stage installs with
  `DESTDIR=$SYSROOT`, and the `$PREFIX` stages are autotools/cmake which honour
  `DESTDIR` too. Composing stages does not require re-plumbing the recipes.
- **The tools we thought we needed are mostly present.** `llvm-dwp` and
  `llvm-profdata` already ship, so split DWARF and PGO are repo-side flag
  decisions, not toolchain work. `llvm-bolt` is genuinely absent. libc++ is
  already a supported switch (`TOOLCHAIN_STDCXX`) that nobody has flipped.
- **The toolchain runs natively on the host.** Its binaries need at most
  `GLIBC_2.30` (gdb; clang and lld need 2.14). Building in a container and
  extracting a tarball to `/opt` is the existing, working model.

## Decisions

**Docker multi-stage with BuildKit is the build engine.** It provides
content-addressed stage caching, `COPY --from=` composition, parallel
scheduling of independent stages, and a shared remote cache - all of which a
bespoke driver would have to reimplement. `container-build.sh` already builds
in a container; today it runs the whole monolith in a single `docker exec`, so
the image caches apt deps and nothing else.

**One script file per tool**, with per-stage version variables. A stage may
depend on exactly four things: its base stage, the artifacts it explicitly
`COPY --from`s, its own version and checksum variables, and its own recipe
text. The build context is this directory, never the repository root -
otherwise editing `src/query/plan.cpp` invalidates GCC. The shared version
block at the top of `build.sh` has to be split for the same reason, or the
first LLVM bump invalidates every stage.

**A justfile is the entry point**, wrapping `docker buildx`. It has no
content-addressed invalidation of its own, so it complements BuildKit rather
than competing with it.

**The native build path is deleted.** Keeping it means two implementations that
drift, and it carries the more dangerous failure mode: a native build on a
modern host links the toolchain against that host's glibc, silently producing
an artifact that will not run on older targets. It also writes directly into
the live `/opt/toolchain-vN` with no override, and its file-existence guards
skip stages whose configuration changed - a change to the LLVM stage is ignored
because `bin/clang` already exists.

**The floors are declared adjacently and enforced.** There are three. Two are
glibc and are currently conflated because both are 2.31: the sysroot glibc,
which sets where *memgraph* runs, and the build container's glibc, which sets
where the *toolchain* runs. The third is the kernel, which applies to both.
Today the invariant tying the glibc pair lives in a comment while the two
values live in different files, and the kernel floor is written out twice -
once as `LINUX_HEADERS_VERSION`, once as a bare "5.4" in glibc's
`--enable-kernel`. Derive the second from the first; a duplicated literal is
how they drift apart.

A verification stage walks every ELF in the assembled prefix and fails the
build if anything exceeds a declared floor. Kernel conformance comes from
`.note.ABI-tag`, which records the oldest kernel an executable runs on. All of
these break silently otherwise: nothing errors at build time, and the failure
surfaces on an older machine as a binary that will not start.

**Artifact-level reproducibility, measured rather than mandated.** The shipped
tarball is currently nondeterministic in four ways: no `--sort=name`, no
`--mtime`, no `--numeric-owner` or pax `delete=atime,delete=ctime`, and `z`
invokes gzip without `-n`. Fix those, set `SOURCE_DATE_EPOCH`, pin the base
image by digest and the apt packages by version. Then build twice and report
what fraction of the 20,765 files differ. Bit-for-bit reproducibility of GCC
and LLVM is a real project and must not become a gate on this one.

**The cache is registry-backed, `mode=max`, and only CI writes to it.** A
compiler cache is a supply-chain surface: anyone who can publish a layer can
publish a backdoored `cc1plus` that every later build silently uses without
rebuilding or inspecting it. Developers read from it and keep full local
caching for their own iteration. Expect tens of GB per version, so retention is
not optional.

**Toolchain selection is configurable, with no hardcoded names.** The Conan
profile is properly templated on `MG_TOOLCHAIN_ROOT`, and then `build.sh`
throws that away by hardcoding `/opt/toolchain-v8` in four places and naming
`memgraph_toolchain_v8` directly. This is independent of everything above and
can land first.

**libc++ ships as a capability, not a supported configuration.** The stdlib
choice belongs in the Conan profile, which already carries
`compiler.libcxx=libstdc++11` as a package-ID input - never baked into clang as
a default. Baking it would let Conan reuse libstdc++-built dependencies while
clang compiled memgraph against libc++, which is an ABI mismatch that surfaces
as a link error at best. The expensive part is not building the runtime; it is
that flipping the setting changes every package ID and requires a parallel set
of dependency binaries.

## Stages

### Stage 1: configurable toolchain selection
**Goal**: select a toolchain by name, with no hardcoded versions anywhere.
**Includes**: replace the four hardcoded `/opt/toolchain-v8` occurrences and
the profile name in `build.sh`; add `conan_config/profiles/memgraph_toolchain_v9`.
**Success**: building against a different installed toolchain requires no edit
to a tracked file.
**Independent of every later stage.**

### Stage 2: feature-free port
**Goal**: reproduce v8 exactly, from Docker multi-stage, adding nothing.
Lives in `builder/`; `v8/build.sh` stays, because stage 3 needs it to generate
the reference tree.

**Landed**: one script per tool with the recipes moved verbatim, one version
file per stage, `archives/` cache mount, deterministic tarball, base image
pinned by digest, the glibc floor gate, and the fingerprint and determinism
tools stage 3 needs.

**Excluded on purpose**: every output-changing change. `llvm-bolt`, libc++, the
gdb rpath fix and zstd all wait for stage 4. The floor gate is the sole
exception, being a check that alters nothing it inspects.

**Deferred, and not yet true**: apt packages are still unpinned, so the builder
can drift even though the image cannot - a snapshot archive closes that. There
is no ccache mount; adding one changes the compiler invocation, which is a poor
thing to do while trying to prove the output unchanged. Stages are chained
rather than fanned out, so the independent leaves do not yet run in parallel.

**Success**: empty structural diff and identical capability fingerprint against
the reference, and memgraph builds and its unit tests pass. Not yet run - that
is stage 3.

### Stage 3: prove equivalence
**Goal**: an oracle that is trustworthy rather than approximate.
**Reference**: rebuild v8 *with the existing bash script* against the
digest-pinned base, and use that tree - not the tarball built in August, whose
base image has since drifted, which would give diffs we cannot attribute.
**Compare**: the sorted relative file list (20,765 entries), the capability
fingerprint (`gcc -v`, `clang -v`, `-print-sysroot`, `-dumpmachine`,
`-print-resource-dir`, LLVMConfig feature flags, per-executable RUNPATH, both
floors), and an end-to-end memgraph build plus unit tests.
**Also**: build twice through the new driver and record the determinism number.

### Stage 4: features, each landing separately
So that each fingerprint delta is readable. Adding `llvm-bolt` should show one
new executable and nothing else; if it shows more, that is a finding.
- zstd in the sysroot, `LLVM_ENABLE_ZSTD=FORCE_ON` (written, verified against v8)
- `llvm-bolt` in `LLVM_ENABLE_PROJECTS`
- libc++ runtime plus a profile, no CI job
- gdb `$ORIGIN`-relative rpath

### Stage 5: version bumps
LLVM 23 and GCC, on a build already proven equivalent, so a failure is
unambiguously a version problem. Needs `compiler.version` updated in the Conan
profile alongside.

## Not in scope

Split DWARF, PGO, precompiled headers, BOLT wiring, LTO, removing PIE and
shared-library unit tests are all repo-side build changes, not toolchain
changes. They are tracked with the branches that carry them. LTO in particular
has never actually been enabled - `CMAKE_INTERPROCEDURAL_OPTIMIZATION_Release`
uses a mixed-case config name that CMake never reads - and turning it on
surfaces ThinLTO link OOM, which is why that work stalled.
