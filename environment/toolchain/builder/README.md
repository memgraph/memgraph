# Toolchain build

Builds toolchain v9 as a set of cached Docker stages instead of one long shell
script, so changing one tool rebuilds that tool and what depends on it rather
than everything.

v8 is not built from here. It stays as it is, in `../v8/`, and this directory
replaces the copy-the-previous-version model for v9 onwards.

```
just build              # build the selected version set, archive lands in output/
just sets               # which toolchains can be built from here
just diff-sets v9 v10   # what the later one changes
just stage llvm         # build up to one stage, for iterating on it
just shell llvm         # shell inside a stage, for debugging a failure
just fingerprint PREFIX # describe an installed toolchain
```

`TC_VERSION_SET` selects the toolchain; it defaults to `v9`.

## Cutting a new toolchain

Copy the version set, edit what changed, build it:

```
cp -r versions/v9 versions/v10
$EDITOR versions/v10/llvm.env
TC_VERSION_SET=v10 just build
```

Nothing else moves. The recipes and the stage graph are shared, so a fix to a
recipe reaches every toolchain built from here at once, which is the thing
copying a directory per version cannot do.

Sets are complete rather than expressing only a delta against their
predecessor. `diff -ru` then answers what changed, with no inheritance to
reason about and no way for a value to move under a version that did not ask
for it -- and a set stays readable top to bottom, which is worth more than the
hundred-odd lines it duplicates.

Only the tools whose version files actually changed rebuild. The version does
not reach the build at all: every stage installs into a fixed prefix and the
packaging stage renames the tree, so bumping a toolchain does not invalidate
the thirty-three stages that did not change. That is also why the version set
is copied into `resolved/` rather than selected with a build argument -- an
argument is an input to every command after it in its stage, so changing one
re-runs every stage even where the version file is byte-identical.

The archive installs and runs on the host exactly as before:

```
tar -xvzf output/toolchain-v9-binaries-x86_64.tar.gz -C /opt
source /opt/toolchain-v9/activate
```

Docker is the build environment, never the runtime. The toolchain's binaries
need at most glibc 2.30, which is why the base image is Ubuntu 20.04 -- it sets
the floor for where the toolchain itself can run.

## Layout

| path | what it is |
|---|---|
| `Dockerfile` | the stage graph; one stage per tool |
| `stages/*.sh` | the build recipes, one per tool |
| `versions/<set>/*.env` | one complete set per toolchain, one file per stage |
| `resolved/` | the selected set, copied here before a build; generated |
| `lib/common.sh` | the environment every stage script expects |
| `files/` | `activate` and `toolchain.cmake`, shipped inside the toolchain |
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

The toolchain promises binaries that run on old systems. Three floors carry
that promise, and `verify/floors.sh` fails the build before packaging if any is
exceeded:

- the **sysroot glibc** decides where binaries built *by* this toolchain run
- the **base image glibc** decides where the toolchain's own binaries run
- the **kernel** floor applies to both, and comes from the headers version

All three break silently otherwise. A binary needing a newer glibc or kernel
does not fail at build time; it fails to start on a machine nobody tested on.

Kernel conformance is read from `.note.ABI-tag`, which records the oldest
kernel an executable will run on and is put there by glibc's
`--enable-kernel`. That setting is derived from `LINUX_HEADERS_VERSION` rather
than written out separately, since the two have to agree and a duplicated
literal is exactly how they drift. Shared objects carry no such note, so an
absent one is not a finding. A note *older* than the floor is not a break
either -- it runs in more places -- but it means the link missed the sysroot's
startup files, so it is counted and reported.

The glibc check carries an explicit exemption list, currently binutils'
gprofng collector libraries: those are built against the host glibc rather than
the sysroot and reference the 2.32 and 2.34 pthread and dl consolidations, so
they will not load on an older target. They are recorded rather than ignored,
the check stays enforcing for everything else, and a stale exemption is itself
an error.

## What is not done here

The base image is pinned by digest, but `apt-get` still resolves against the
live archive, so builder package versions can drift even when the image does
not. A snapshot archive is what closes that.

v9 starts from v8's package versions on purpose, so that the restructure can
be checked against a toolchain we already trust: build v8 with `../v8/build.sh`,
build this, and compare the two with `just compare`. A version bump on top of an
unproven restructure has two candidate causes when it fails, and this has one.

So the versions here are a starting point, not the shipping set. What v9 adds to
v8 is already present -- mold, BOLT, dwz, libabigail, zstd-compressed debug info
and relative runpaths -- and the LLVM and GCC bumps land after the comparison
above has been run.
