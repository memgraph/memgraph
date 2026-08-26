#!/usr/bin/env python3
"""Find variables a stage reads that nothing gives it.

Splitting one shell script into a process per stage breaks every implicit
dependency the single shell used to satisfy, and those breaks surface hours
into a build rather than at the point of the mistake. Two did: curl needed
exports that died with the shell they were set in, and llvm read SWIG_VERSION
without sourcing swig.env. A third was waiting in the packaging stage, which
writes a README naming every tool's version and sourced none of them.

Every stage runs under `set -u`, so an unbound variable is fatal and this is
worth checking before starting a build rather than after.

Run: verify/stage-vars.py   (or `just check`)
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def assigned_in(text):
    """Variables a script sets: plain assignment, loop variable, or read.

    declare and its options count as assignment too; an associative array has
    to be declared before it can be used, so the declaration is the only place
    its name appears on the left.
    """
    return (
        set(re.findall(r"^\s*(?:export\s+|local\s+|declare\s+(?:-\w+\s+)*)?([A-Za-z_][A-Za-z0-9_]*)=", text, re.M))
        | set(re.findall(r"\bfor\s+([A-Za-z_][A-Za-z0-9_]*)\s+in\b", text))
        | set(re.findall(r"\bread\s+(?:-r\s+)?([A-Za-z_][A-Za-z0-9_]*)", text))
    )


def sourced_env_vars(text):
    """Variables provided by the version files a script sources."""
    out = set()
    for env in re.findall(r"\$TC_VERSIONS/([a-z0-9-]+)\.env", text):
        f = ROOT / "versions" / f"{env}.env"
        if f.exists():
            out |= assigned_in(f.read_text())
    return out


common = (ROOT / "lib/common.sh").read_text()
clang_env = (ROOT / "lib/clang-env.sh").read_text()
from_common = assigned_in(common) | sourced_env_vars(common)
from_clang = assigned_in(clang_env)

# provided by the shell or by the Dockerfile's ENV, not by any script here
AMBIENT = {
    "PATH",
    "PWD",
    "HOME",
    "ORIGIN",
    "IFS",
    "LD_LIBRARY_PATH",
    "CFLAGS",
    "CXXFLAGS",
    "LDFLAGS",
    "CPPFLAGS",
    "DESTDIR",
    "MAKEFLAGS",
    "TC_FOR_ARM",
    "TC_DISTRO",
    "TC_BUILDER_GLIBC_FLOOR",
    "SOURCE_DATE_EPOCH",
    "TOOLCHAIN_STDCXX",
    "TERM",
    "LANG",
    "LC_ALL",
    "TZ",
}

problems = []
for sh in sorted((ROOT / "stages").glob("*.sh")):
    text = sh.read_text()
    provided = from_common | AMBIENT | assigned_in(text) | sourced_env_vars(text)
    if "clang-env.sh" in text:
        provided |= from_clang
    # comments do not execute, so a variable named in one is not a dependency
    code = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))
    used = set(re.findall(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?", code))
    missing = sorted(v for v in used - provided if not v.startswith("BASH"))
    if missing:
        problems.append((sh.name, missing))

for name, missing in problems:
    print(f"  {name}: {' '.join(missing)}")

if problems:
    print(f"\n{len(problems)} stage(s) read something nothing provides")
    sys.exit(1)
print(f"all {len(list((ROOT / 'stages').glob('*.sh')))} stages have every variable they read")
