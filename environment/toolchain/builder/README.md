# Toolchain build

Builds the memgraph toolchain as a set of cached Docker stages instead of one
long shell script, so changing one tool rebuilds that tool and what depends on
it rather than everything.

```
just build              # build the toolchain, archive lands in output/
just stage llvm         # build up to one stage, for iterating on it
just shell llvm         # shell inside a stage, for debugging a failure
just fingerprint PREFIX # describe an installed toolchain
```

The archive installs and runs on the host exactly as before:

```
tar -xvzf output/toolchain-v8-binaries-x86_64.tar.gz -C /opt
source /opt/toolchain-v8/activate
```

Docker is the build environment, never the runtime. The toolchain's binaries
need at most glibc 2.30, which is why the base image is Ubuntu 20.04 -- it sets
the floor for where the toolchain itself can run.

## Layout

| path | what it is |
|---|---|
| `Dockerfile` | the stage graph; one stage per tool |
| `stages/*.sh` | the build recipes, moved verbatim from `../v8/build.sh` |
| `versions/*.env` | one file per stage, so a bump invalidates only that stage |
| `lib/common.sh` | the environment every stage script expects |
| `files/` | `activate.in` and `toolchain.cmake` for the built toolchain |
| `verify/` | the glibc floor gate, the fingerprint, the determinism measurement |
| `justfile` | argument assembly; BuildKit owns the graph and the caching |

## Why it is shaped this way

**One script per tool, one version file per tool.** A stage may depend only on
its base stage, the artifacts it copies, its own version file and its own
recipe. If the build context were the repository, editing any source file would
rebuild the compiler; if one file held every version, bumping LLVM would
invalidate GCC.

**Chained, not fanned out.** The bootstrap is genuinely a chain: glibc needs
the kernel headers, GCC needs glibc, everything after needs GCC. The recipes
also reach across the shared build directory -- gmp and mpfr build from GCC's
unpacked source tree, and LLVM runs the swig that the swig stage built. That is
why `$DIR` is one directory holding `archives/`, `build/` and the toolchain's
own files, exactly as it was in the original script. Fanning out the
independent leaves is worth doing once the port is proven, not before.

**Recipes were moved, not rewritten.** Those commands encode expensive lessons
about bootstrap ordering, target triples and sysroot layout. The extraction was
mechanical and checked for coverage: every recipe line landed in exactly one
stage, with no overlaps.

## Floors

Two glibc floors, independent of each other, both enforced before packaging:

- the **sysroot** glibc decides where binaries built *by* this toolchain run
- the **base image** glibc decides where the toolchain's own binaries run

`verify/glibc-floor.sh` fails the build if either is exceeded. It carries an
explicit exemption list, currently binutils' gprofng collector libraries: those
are built against the host glibc rather than the sysroot and reference the 2.32
and 2.34 pthread and dl consolidations, so they will not load on an older
target. They are recorded rather than ignored, the check stays enforcing for
everything else, and a stale exemption is itself an error.

## What is not done here

The base image is pinned by digest, but `apt-get` still resolves against the
live archive, so builder package versions can drift even when the image does
not. A snapshot archive is what closes that.

The port targets v8's versions on purpose. It has to reproduce the existing
toolchain before anything is added or bumped, or the comparison that proves it
correct has nothing to compare against. `../v8/build.sh` stays for exactly that
reason: it generates the reference tree.
