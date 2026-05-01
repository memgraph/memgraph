# memgraph-debuginfo packaging

Long-term: have a debuginfod-style symbol server so a developer with a
core dump just needs the build-id and gets debug info on demand. Short-term:
ship a separate `memgraph-debuginfo` package alongside `memgraph` so the
.dwp is recoverable per build.

## Stage 1: Foundation (CMake)
**Goal**: Get `.dwp` into a separate install component and fix the POST_BUILD
ordering bug, with the existing single-component DEB/RPM output unchanged.

**Success Criteria**:
- For RelWithDebInfo and Release, dwp runs *before* strip.
- `install(... COMPONENT debuginfo)` exists for the .dwp.
- Existing `cpack -G DEB` / `-G RPM` still produces one package containing
  the binary + everything else (no debuginfo file in it). Components don't
  split unless `CPACK_*_COMPONENT_INSTALL` is set.
- All current install rules tagged `COMPONENT memgraph` explicitly so future
  componentization isn't ambiguous.

**Status**: Complete

## Stage 2: DEB component split
**Goal**: Produce `memgraph_<ver>_<arch>.deb` + `memgraph-debuginfo_<ver>_<arch>.deb`.

**Success Criteria**:
- `cpack -G DEB` produces two `.deb` files locally.
- `memgraph-debuginfo` depends on exact version of `memgraph`.
- `dpkg -c memgraph-debuginfo*.deb` shows `usr/lib/memgraph/memgraph.dwp`
  and nothing else functional.
- `dpkg -c memgraph*.deb` no longer contains the .dwp (verify size shrank).
- Installing both makes gdb auto-find the debug info.

**Status**: Complete

## Stage 3: RPM component split
**Goal**: Same as Stage 2 for `.rpm`. Tricky because the project uses a
hand-rolled spec template (`release/rpm/memgraph.spec.in`).

**Investigate first**: can we drop the custom spec for the debuginfo
component (CPack auto-generates), or do we need a second `.spec.in`?
The `%prep` surgery (systemd unit move, perms) only applies to the main
package; debuginfo just contains a single file at a regular path.

**Success Criteria**:
- `cpack -G RPM` produces two `.rpm` files.
- `rpm -qlp memgraph-debuginfo*.rpm` contains only the .dwp.
- Existing rpmlint runs in mgbuild.sh still pass.

**Status**: Complete. Verified end-to-end via fedora:40 container:
build rpms, install both, gdb resolves source via .dwp; negative test
(hide .dwp) restores "Could not find DWO CU" warning.

## Stage 4: CI / GH workflow updates
**Goal**: Both packages flow through `package_memgraph.yaml` / `reusable_package.yaml`
and end up uploaded as artifacts.

**Success Criteria**:
- `mgbuild.sh copy --package` picks up both files.
- Workflow `actions/upload-artifact` includes both packages.
- Decision documented (in commit message) on whether debuginfo gets pushed
  to `download.memgraph.com` or stays internal.

**Status**: Not Started

## Stage 5 (later): Symbol server / debuginfod
**Goal**: CI uploads `.dwp` keyed by Build-ID to a debuginfod-compatible
endpoint; developers configure `DEBUGINFOD_URLS` and stop carrying packages.

**Out of scope for this PR.** Captured here so we don't lose the thread.

**Status**: Not Started
