#!/usr/bin/env python3
"""Assert the hashlib gate is active in this interpreter.

Run inside the memgraph container by the smoke suite's test_fips_hashlib_gate,
both directly (the interpreter the auth-module subprocesses use) and, via a
generated query module, inside memgraph's embedded interpreter.

The gate stands itself down on any error -- a failed import, a broken .pth, or
an OpenSSL that claims approved mode but will not serve SHA-256 -- and leaves
hashlib untouched. That is deliberate, so a bad configuration cannot stop the
interpreter starting, but it means the only evidence is a line on stderr that
nothing reads. A build where the .pth is simply missing produces no signal at
all. So the outcome has to be asserted rather than assumed.
"""

import hashlib
import sys


def check():
    """Return a list of failure strings; empty means the gate is doing its job."""
    problems = []

    # sys.modules, not import: importing the module runs it, which would engage
    # the gate here and hide the very failure being looked for. If the .pth ran,
    # the module is already loaded before this script starts.
    gate = sys.modules.get("memgraph_fips_hashlib")
    if gate is None:
        return ["memgraph_fips_hashlib did not run at interpreter start (zz-memgraph-fips.pth missing, or it failed)"]

    if not gate.engaged:
        problems.append("gate imported but engaged is False (it stood down; see stderr at startup)")

    # Approved digests must still come from OpenSSL, not a builtin fallback.
    if hashlib.sha256.__module__ != "_hashlib":
        problems.append(f"sha256 comes from {hashlib.sha256.__module__}, not _hashlib")
    try:
        hashlib.sha256(b"x").hexdigest()
    except Exception as exc:  # pragma: no cover
        problems.append(f"sha256 failed: {type(exc).__name__}: {exc}")

    # MD5 must raise rather than quietly return a builtin digest.
    try:
        digest = hashlib.md5(b"x").hexdigest()
    except ValueError:
        pass
    else:
        problems.append(f"hashlib.md5 returned {digest} instead of raising")

    # ...and must not still be advertised as available.
    if "md5" in hashlib.algorithms_available:
        problems.append("md5 is still listed in hashlib.algorithms_available")

    # hashlib.new must agree with the attribute constructors.
    try:
        hashlib.new("md5", b"x")
    except Exception:
        pass
    else:
        problems.append('hashlib.new("md5") succeeded')

    return problems


if __name__ == "__main__":
    failures = check()
    if failures:
        for failure in failures:
            print(f"      {failure}", file=sys.stderr)
        sys.exit(1)
    print("  gate engaged: sha256 from _hashlib, md5 refused and unadvertised")
