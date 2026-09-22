#!/usr/bin/env python3
"""Remove an entry from a CMakeCache.txt.

profile.sh injects launcher.cmake through CMAKE_PROJECT_INCLUDE, which CMake
keeps in the cache. Left there, a later configure would fail as soon as the file
went missing (a branch switch, say), so the driver forgets the entry when it is
done. The entry's help comment goes with it: CMake rejects a cache file whose
comment has no entry beneath it.

    cmake_cache.py forget CMakeCache.txt ENTRY_NAME
"""
import argparse
import re
import sys


def forget(cache_path, name):
    with open(cache_path) as f:
        lines = f.readlines()
    idx = next((i for i, line in enumerate(lines) if re.match(rf"^{re.escape(name)}[:=]", line)), None)
    if idx is None:
        return False
    start = idx
    while start > 0 and lines[start - 1].startswith("//"):
        start -= 1
    del lines[start : idx + 1]
    with open(cache_path, "w") as f:
        f.writelines(lines)
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="command", required=True)
    p = sub.add_parser("forget", help="delete ENTRY_NAME and its help comment")
    p.add_argument("cache", help="path to CMakeCache.txt")
    p.add_argument("name", help="cache entry name, e.g. CMAKE_PROJECT_INCLUDE")
    args = ap.parse_args()
    forget(args.cache, args.name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
