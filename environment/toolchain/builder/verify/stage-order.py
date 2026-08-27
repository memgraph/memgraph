#!/usr/bin/env python3
"""Check the Dockerfile runs the stages in the order declared here.

Some dependencies between stages are invisible in the recipes: the stages that
build against the finished toolchain source $PREFIX/activate, which an earlier
stage writes. Grouping that with packaging, where it looks like it belongs,
moved it after its consumers and broke a build an hour in.

Reordering is legitimate, but it should be a decision rather than an accident,
so this fails on any divergence and expects the list below to be edited
deliberately. Adding a stage means adding it here, in the position it has to
run in, with the reason if that is not obvious.

Run: verify/stage-order.py   (or `just check`)
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# The order stages run in. Where a stage's position is forced by something the
# recipes do not show, the reason is next to it.
DECLARED_ORDER = [
    # Bootstrap. Each genuinely needs the one before it.
    "linux-headers",
    "glibc",
    "gcc",
    "gcc-sysroot-libs",
    "gmp",  # builds out of gcc's unpacked source tree
    "mpfr",  # likewise
    "binutils",
    # Sysroot libraries, for the tools built below.
    "zlib",
    "ncurses",
    "openssl",
    "curl",
    "libffi",
    "elfutils",  # needed by dwz and libabigail
    "libxml2",  # needed by libabigail
    "xxhash",  # needed by dwz since its 0.16
    "zstd",  # needed by llvm for -gz=zstd
    "xz",  # required outright by libabigail
    "dwz",  # before llvm so a bump does not miss it
    "libabigail",
    "python",  # gdb links it for scripting support
    # Tools.
    "cmake",
    "mold",  # only needs cmake, and is before llvm so that link uses it
    "libipt",
    "gdb",
    "pahole",
    "gdbinit",  # writes the init file that loads pahole
    "cppcheck",
    "swig",  # llvm runs the swig this installs
    "llvm",
    # Built against the finished toolchain, so the activation script has to
    # exist before them.
    "activate",
    "mgconsole",
    "heaptrack",
    # Check, then rewrite, then ship.
    "verify",  # alters nothing it inspects, so it runs before packaging
    "relocate",  # rewrites the finished tree
    "package",  # renames and archives it, and is the last word
]

dockerfile = (ROOT / "Dockerfile").read_text()
built = re.findall(r"^FROM\s+\S+\s+AS\s+s-(\S+)", dockerfile, re.M)

problems = []

undeclared = [s for s in built if s not in DECLARED_ORDER]
if undeclared:
    problems.append("not in DECLARED_ORDER, so nothing checks where they run: " + " ".join(undeclared))

missing = [s for s in DECLARED_ORDER if s not in built]
if missing:
    problems.append("declared but no Dockerfile stage: " + " ".join(missing))

if not problems:
    expected = [s for s in DECLARED_ORDER if s in built]
    if built != expected:
        for i, (got, want) in enumerate(zip(built, expected)):
            if got != want:
                problems.append(f"stage {i + 1} is s-{got}, declared order says s-{want}")
                break

for p in problems:
    print(f"  {p}")
if problems:
    sys.exit(1)
print(f"all {len(built)} stages run in the declared order")
