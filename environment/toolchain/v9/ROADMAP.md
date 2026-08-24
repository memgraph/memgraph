# Toolchain v9 kickoff

This directory starts as a copy of v8 with the version markers bumped and two
changes already applied. Everything else below is a candidate, gathered from
branches that carry unmerged build work, so the decisions can be made in one
place rather than rediscovered.

## Already applied here

**zstd in the sysroot, required in LLVM.** v8 was not built against libzstd, so
`-gz=zstd` fails: clang only warns, lld hard-errors. The LLVM stage sets
`CMAKE_FIND_ROOT_PATH_MODE_*=ONLY`, so its `find_package` only ever sees the
sysroot, and `LLVM_ENABLE_ZSTD` defaults to ON meaning *auto-detect*, so it
silently disabled itself. zstd is now built static into `$SYSROOT/usr` next to
zlib, and `LLVM_ENABLE_ZSTD=FORCE_ON` makes a missing zstd a configure error
instead of a silent downgrade. v4 built zstd; v5 dropped it.

This was built and verified in a container against v8: `LLVMConfig.cmake` gets
`LLVM_ENABLE_ZSTD TRUE`, and `-gz=zstd` produces sections with `ch_type=2`
(`ELFCOMPRESS_ZSTD`) that gdb and llvm-dwarfdump both read.

**Signing keys fetched over hkps.** The script asked for keys from a bare
`keyserver.ubuntu.com`, which means plain HKP on port 11371. From inside a
build container that port accepts the TCP connection and then never answers,
and gpg applies no timeout, so the build wedges silently on the very first key
fetch with no output. `hkps://` moves it to 443.

## Version bumps to decide

Left at v8's values deliberately, so this directory builds as-is and the bumps
are a separate, reviewable step:

| | v8 | v9 |
|---|---|---|
| LLVM | 22.1.8 | 23.x - the reason for v9 |
| GCC | 16.2.0 | ? |
| binutils | 2.47 | ? |
| GDB | 17.2 | ? |
| CMake | 4.4.1 | ? |
| glibc | 2.31 | ? |

Note that binutils' `objcopy` cannot read zstd-compressed sections at all, and
GNU `ld` rejects `-gz=zstd`. Neither is on our path today (the toolchain file
pins `CMAKE_OBJCOPY` to `llvm-objcopy` and we link with lld), but a bump that
changes either default would matter.

## Build-process candidates

These are changes to the repository's CMake, not to the toolchain. They are
listed here because they interact with the toolchain and with each other, and
because several are already written and stalled.

### LTO has never actually been on

`CMAKE_INTERPROCEDURAL_OPTIMIZATION_Release` and `_RelWithDebInfo` use
mixed-case config names. CMake only reads the upper-case `_RELEASE` /
`_RELWITHDEBINFO` forms, so the target property is never set and no `-flto`
reaches any translation unit - despite `check_ipo_supported` being a hard
configure-time error, which is what makes it look enabled. Fixed on
`2026_05_01_split_dwarf` (PR #4102, draft).

Turning it on is what surfaced the rest: lld defaults `--thinlto-jobs` to
`nproc`, and each codegen job wants 1-3 GB, so a single link can OOM a shared
runner. Master's job pools (`cmake/BuildParallelism.cmake`) bound how many
links run at once but not the parallelism *inside* one link, so the cap is
still needed.

### Debug info

| item | where | note |
|---|---|---|
| compressed DWARF (`-gz`) | PR #4640 | merging; `auto` probes and falls back zstd -> zlib -> none |
| split DWARF (`-gsplit-dwarf` + `.dwp`) | PR #4102 | lld numbers `.dwo` files by task ID from 1 per link, so a shared `dwo_dir` lets concurrent test links truncate-overwrite memgraph's; must be per-target |
| `--gdb-index` | PR #4102 | prebuilt index |
| `-frecord-command-line` | PR #4102 | recovers build flags from a crash artifact |
| `-ffile-prefix-map` | PR #4102 | reproducible paths, ccache hits across checkouts; needs an identity rule for `CMAKE_BINARY_DIR` after the source rule, or skeleton CUs get relative `DW_AT_GNU_dwo_name` that `llvm-dwp` cannot resolve |
| split debug sidecars | in master | `cmake/SplitDebug.cmake` |

`-gz` and split DWARF overlap: both attack the same duplicated DWARF. Worth
deciding whether they are complementary or whether one supersedes the other
before landing both.

### Optimisation

| item | where | blocker |
|---|---|---|
| PGO (`MG_PGO=generate\|use`) | PR #4325 | experiment, needs a training workload story |
| BOLT | `facebook-bolt` | no PR; needs non-PIE |
| precompiled headers (`REUSE_FROM`) | PR #4055 | experiment |
| drop librdtsc | PR #4637 | in review |

### Build shape

- **Remove PIE.** `facebook-bolt` does this. Also unblocks shared libraries for
  the unit tests: `mg-*` objects are `-fPIE`, so they cannot go into a `.so`.
- **Shared libraries for unit tests.** The unit-test tree duplicates library
  DWARF across ~214 executables. Blocked by four dependency cycles
  (flags/license/requests; replication/rpc/communication/auth/system;
  replication_handler/dbms/query/coordination; dbms/query) - CMake permits
  cycles only among static libraries - plus the PIE problem above, plus wanting
  memgraph itself to stay statically linked. `feat/break-deps` starts on the
  cycles.
- **Conan 2.24.0** - `update-conan`.
- **macOS native build** - `exp/macos-native-build`.
- **libc++ instead of libstdc++** - `compile-with-llvm-stdlib` is from 2022 and
  stale, but worth reconsidering: GCC 16's libstdc++ doubled `std::regex` cost
  and that showed up as a query-throughput regression.
- **C++ modules** - `feat/improve-modules-build-time`.
