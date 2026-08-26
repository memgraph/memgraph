#!/usr/bin/env python3
"""Check the stages run in the order the original script ran them.

The original was one long file, so ordering was whatever the line order said
and dependencies between stages never had to be written down. Some of those
dependencies are invisible in the recipes: the stages that build against the
finished toolchain source $PREFIX/activate, which an earlier part of the script
writes. Grouping that with packaging, where it looks like it belongs, moved it
after its consumers and broke a build an hour in.

Reordering is legitimate, but it should be a decision rather than an accident,
so this fails on any reordering and expects the table below to be updated
deliberately.

Run: verify/stage-order.py   (or `just check`)
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# where each stage's recipe began in environment/toolchain/v8/build.sh
ORIGINAL_ORDER = {
    "linux-headers": 239,
    "glibc": 256,
    "gcc": 302,
    "gcc-sysroot-libs": 406,
    "gmp": 437,
    "mpfr": 460,
    "binutils": 481,
    "zlib": 557,
    "ncurses": 571,
    "openssl": 601,
    "curl": 621,
    "libffi": 647,
    "python": 664,
    "cmake": 694,
    "libipt": 733,
    "gdb": 761,
    "pahole": 840,
    "gdbinit": 847,
    "cppcheck": 873,
    "swig": 898,
    "llvm": 920,
    "activate": 1002,
    "mgconsole": 1088,
    "heaptrack": 1118,
    "package": 1167,
}

# Stages added since the port, which have no line in the original. Each names
# the stage it must follow, so its position is still checked rather than
# exempt, and the reason it sits where it does is written down.
ADDED_AFTER = {
    "elfutils": "libffi",  # sysroot library, needed by dwz and libabigail
    "libxml2": "elfutils",  # sysroot library, needed by libabigail
    "xxhash": "libxml2",  # sysroot library, needed by dwz since its 0.16
    "xz": "xxhash",  # sysroot library, required outright by libabigail
    "dwz": "xz",  # needs elfutils and xxhash; before llvm so a bump misses it
    "libabigail": "dwz",  # needs elfutils and libxml2
    "mold": "cmake",  # needs only cmake; before llvm so that link can use it
}

for _name, _after in ADDED_AFTER.items():
    ORIGINAL_ORDER[_name] = ORIGINAL_ORDER[_after] + 1

dockerfile = (ROOT / "Dockerfile").read_text()
built = [m for m in re.findall(r"^FROM\s+\S+\s+AS\s+s-(\S+)", dockerfile, re.M) if m in ORIGINAL_ORDER]

problems = []
for earlier, later in zip(built, built[1:]):
    if ORIGINAL_ORDER[earlier] > ORIGINAL_ORDER[later]:
        problems.append(
            f"s-{later} (build.sh:{ORIGINAL_ORDER[later]}) runs after "
            f"s-{earlier} (build.sh:{ORIGINAL_ORDER[earlier]}), but came first originally"
        )

missing = sorted(set(ORIGINAL_ORDER) - set(built))
if missing:
    problems.append(f"no Dockerfile stage for: {' '.join(missing)}")

for p in problems:
    print(f"  {p}")
if problems:
    sys.exit(1)
print(f"all {len(built)} stages run in the original order")
