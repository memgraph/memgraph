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
lines it duplicates.

Only the tools whose version files actually changed rebuild. The version does
not reach the build at all: every stage installs into a fixed prefix and the
packaging stage renames the tree, so bumping a toolchain does not invalidate
the stages that did not change. That is also why the version set
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
| `versions/<set>/builder.env` | the base image and the floor for where the toolchain runs |
| `versions/<set>/packages.txt` | what the build machine needs, and nothing else |
| `resolved/` | the selected set, copied here before a build; generated |
| `lib/common.sh` | the environment every stage script expects |
| `files/` | `activate` and `toolchain.cmake`, shipped inside the toolchain |
| `stages/manifest` | what each stage needs and copies; the Dockerfile is checked against it |
| `verify/` | the floor and runtime-link gates, the stage-graph check, the fingerprint and determinism tools |
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

**A version set owns its build environment too.** The base image and the floor
for where the toolchain itself runs live in `builder.env`, next to each other
and next to the versions, because they are one decision; that file says why.
The justfile passes them before any argument you add, so `--build-arg
BASE_IMAGE=...` still wins and a one-off build on a different base does not
need a version set of its own.

**When a recipe has to differ between versions.** It has not happened yet, so
there is no machinery for it, and this is the rule for when it does. Condition
on the tool's own version rather than the toolchain's: a flag exists because
LLVM 23 accepts it and 22 does not, which stays true the next time LLVM is
bumped, whereas "from v10 onwards" stops being true as soon as v11 exists. Once
a recipe carries more than a couple of these, fork it deliberately and say in
both copies that the other exists -- a recipe that is half conditional is
harder to read than two recipes.

**The build commands came from the shipped v8 script.** They encode expensive
lessons about bootstrap ordering, target triples and sysroot layout, so the
extraction was mechanical and checked for coverage: every recipe line landed in
exactly one stage, with no overlaps. Change what a recipe builds when there is a
reason to; do not tidy one into a shape that reads better.

**Fetching and unpacking are shared, the rest is not.** `fetch` takes a URL and
a digest and checks it on every build, not only when it downloads, because the
archive cache outlives the build. `enter_source` unpacks and leaves the shell in
the tree, and `leave_source` returns, so a recipe cannot leave the directory
stack unbalanced -- which nothing would report, since it surfaces as a stage
building in the wrong place. What each tool actually does is left in its own
recipe.

**The stage graph is declared.** `stages/manifest` says what each stage depends
on, what it copies beyond its own recipe and version file, and whether it gets
the download cache; `verify/stage-graph.py` refuses a Dockerfile that disagrees.
Order is checked as a constraint, so a stage must run after everything it needs
and two stages that need nothing of each other may be in either order.
`just impact <tool>` reports what changing one would rebuild.

**There is no path that builds outside a container.** Two implementations
drift, and the one that runs on the host carries the worse failure: it links
the toolchain against whatever glibc that host has, producing an artifact that
runs there and not on an older target, with nothing to say so. The old script
also wrote straight into the live install prefix with no override, and skipped a
stage whose configuration had changed whenever the file that stage installs was
already present.

**Every source is fixed to something that cannot move.** A git tag is a movable
pointer, so the three stages that clone one check the commit they landed on
afterwards, and a tag that has moved fails the build. Everything else is pinned
by digest or verified by signature.

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

The glibc check can record a file as knowingly above the floor rather than
failing on it, so a deliberate exception stays visible and the check stays
enforcing for everything else. An exemption for a file that is no longer built
is itself an error. Nothing is exempt; `floors.sh` says what used to be and
why.

## What is not done here

The base image is pinned by digest, but `apt-get` still resolves against the
live archive, so builder package versions can drift even when the image does
not. A snapshot archive is what closes that.

v9 starts from v8's package versions on purpose, so that the restructure can
be checked against a toolchain we already trust: build v8 with `../v8/build.sh`,
build this, and compare the two with `just compare`. That comparison has not
been run. The build itself has: it completes from an empty cache, passes its
gates, and produces a toolchain that builds memgraph. What is missing is the
evidence that it produces the *same* toolchain rather than merely a working one.
A version bump on top of an unproven restructure has two candidate causes when
it fails, and this has one.

libc++ is buildable but not a supported configuration. The standard library is
a package-id input in the conan profile and must stay there rather than being
baked into clang as a default: baked, conan would reuse libstdc++-built
dependencies while clang compiled memgraph against libc++, and the mismatch
surfaces as a link error at best. The cost is not building the runtime, it is
that flipping the setting needs a parallel set of dependency binaries.

So the versions here are a starting point, not the shipping set. What v9 adds to
v8 is already present -- mold, BOLT, dwz, libabigail, zstd-compressed debug info
and relative runpaths -- and the LLVM and GCC bumps land after the comparison
above has been run.
