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

Docker is the build environment, never the runtime. The base image is CentOS
Stream 9 because the toolchain's own binaries link against the base image's
glibc, so the base decides where the toolchain can run -- and CentOS Stream 9's
2.34 is the oldest glibc of any distro we package for. Anything newer, Ubuntu
22.04 included at 2.35, would produce a toolchain that cannot start in the
CentOS Stream 9 container our own packages are built in.

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

The glibc check can carry an exemption list, and it is currently empty. An
entry there is a promise the toolchain does not keep, so a stale one is itself
an error rather than something to leave lying around.

## Where the floors come from

`verify/supported-floors.sh` answers that from the distros we package for: it
reads the target list from `SUPPORTED_OS_V8` so it cannot disagree with what CI
builds, and reads glibc and the packaged kernel from the distro images rather
than a published table -- one such table gives Rocky 10.2 as glibc 2.44 where
the image has 2.39. CentOS Stream 9 is the oldest on both counts, at glibc 2.34
and kernel 5.14, and that is what the sysroot and the base image are set to.

There is no reason to build below that. It buys compatibility for systems
nobody supports, and it is what previously pinned the builder to Ubuntu 20.04.

## What is not done here

The base image is pinned by digest, but `apt-get` still resolves against the
live archive, so builder package versions can drift even when the image does
not. A snapshot archive is what closes that.

The port targets v8's versions on purpose. It has to reproduce the existing
toolchain before anything is added or bumped, or the comparison that proves it
correct has nothing to compare against. `../v8/build.sh` stays for exactly that
reason: it generates the reference tree.
