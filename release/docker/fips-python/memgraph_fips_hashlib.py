"""Stop hashlib substituting unvalidated hash implementations in approved mode.

Imported at interpreter start by zz-memgraph-fips.pth, so it applies to the
embedded interpreter that runs Python query modules *and* to the auth-module
subprocesses (src/auth/module.cpp forks and execs python3, which runs site
initialisation like any other interpreter).

WHY THIS EXISTS

CPython's hashlib picks an implementation per digest at import time by probing
OpenSSL, and falls back to its own _md5/_sha1/_sha2/_sha3 modules when OpenSSL
refuses. Those are outside the validated cryptographic module.

In approved mode the FIPS provider supplies SHA-1, SHA-2, SHA-3 and SHAKE, so
those keep coming from the validated module. MD5 it does not supply at all, so
`hashlib.md5` silently becomes CPython's `_md5`: MD5 stays fully available, and
is still advertised in `hashlib.algorithms_available`, inside an image that has
otherwise removed MD5 from OpenSSL entirely. The same applies to any algorithm
a future validated provider drops -- it reappears here with no signal.

This module makes that fail loudly instead. It does not attempt to sandbox
Python: `import _md5` still works, and no amount of gating can stop arbitrary
user code from computing an unapproved digest. What it removes is the *silent*
path, and the false capability advertised by `algorithms_available`.

HOW IT DECIDES

It does not try to work out for itself which algorithms OpenSSL will serve.
That is what an earlier version did -- probing with `_hashlib.new(name)` -- and
it was wrong: hashlib selects its constructors with the property query "-fips",
which *drops* the fips requirement, while a bare `_hashlib.new()` inherits
`default_properties = fips=yes`. Under a half-configured OpenSSL (a default
provider active but `fips=yes` demanded, so nothing satisfies the strict query
while the loose one succeeds) the two disagree, and the probe concluded that
*no* algorithm was available and replaced every constructor -- including
SHA-256 -- with a stub.

So instead it asks hashlib what it already decided, which cannot disagree with
itself: a constructor whose `__module__` is `_hashlib` came from OpenSSL and is
left alone; one that came from `_md5`/`_sha1`/`_sha2`/`_sha3` is exactly the
unvalidated fallback this module exists to remove.

`hashlib.new()` is routed straight at `_hashlib.new()` for the same reason --
that call inherently cannot fall back, so no separate allow-list is needed.

SCOPE

Inert unless OpenSSL is actually in approved mode, so the same file is safe to
ship in the non-FIPS image. It also stays inert if approved mode is claimed but
OpenSSL will not serve SHA-256, because that is a broken configuration in which
there is no validated module to protect and clamping down would only break the
interpreter. Memgraph refuses to start in that state anyway
(communication/init.cpp checks OSSL_PROVIDER_available before enabling).

Unapproved constructors become raising stubs rather than being deleted.
Deleting them breaks `from hashlib import md5` at module import time, which
pip's vendored urllib3 does in util/ssl_.py -- it builds a
{32: md5, 40: sha1, 64: sha256} table without calling md5 -- so a delete makes
pip itself unimportable in approved mode.

BLAKE2 is deliberately kept. hashlib always routes blake2 to CPython's builtin
because OpenSSL's blake2 cannot do keyed or variable-length digests, and
networkx does `from hashlib import blake2b` at import time -- removing it makes
`import networkx` fail, which breaks every Python query module. BLAKE2 is not
an approved algorithm, so it is never a *substitute* for one: reaching for it is
always the caller's explicit choice, never a silent downgrade.
"""

import os
import sys

__all__ = ["engaged", "kept", "removed"]

#: True when the gate actually restricted hashlib.
engaged = False
#: Constructors left in place because OpenSSL serves them (validated).
kept = ()
#: Constructors replaced with raising stubs (the unvalidated fallbacks).
removed = ()

_BUILTIN_HASH_MODULES = frozenset(
    {"_md5", "_sha1", "_sha2", "_sha3", "_sha256", "_sha512"}
)


def _is_blake2(name):
    return name.lower().replace("-", "_").startswith("blake2")


def _make_stub(name):
    def stub(data=b"", *, usedforsecurity=True):
        raise ValueError(
            "%s is not available from the OpenSSL FIPS provider; this "
            "interpreter refuses to substitute an implementation outside the "
            "validated module" % (name,)
        )

    stub.__name__ = name
    stub.__qualname__ = name
    stub.__doc__ = (
        "Unavailable in OpenSSL approved mode. Raises ValueError.\n\n"
        "Kept as an attribute rather than deleted so that a top-level\n"
        "`from hashlib import %s` still imports; several libraries do that\n"
        "and only use the name conditionally." % (name,)
    )
    return stub


def _install(_hashlib, hashlib):
    global engaged, kept, removed

    _orig_new = hashlib.new

    def new(name, data=b"", **kwargs):
        # blake2 never comes from OpenSSL; everything else goes straight to
        # OpenSSL, which inherently cannot fall back to a builtin.
        if _is_blake2(name):
            return _orig_new(name, data, **kwargs)
        return _hashlib.new(name, data, **kwargs)

    new.__doc__ = getattr(_orig_new, "__doc__", None)
    hashlib.new = new

    keep, drop = [], []
    for name in sorted(hashlib.algorithms_guaranteed):
        fn = getattr(hashlib, name, None)
        if fn is None:
            continue
        if _is_blake2(name) or getattr(fn, "__module__", "") not in _BUILTIN_HASH_MODULES:
            # Either blake2, or served by OpenSSL -- leave it exactly as is.
            keep.append(name)
            continue
        # hashlib fell back to its own implementation for this digest.
        setattr(hashlib, name, _make_stub(name))
        drop.append(name)

    # Keep the advertised sets honest, including OpenSSL-specific names that
    # have no hashlib attribute (md5-sha1, ripemd160, sm3, ...). The probe here
    # is the same call hashlib.new() now makes, so the two cannot disagree.
    for name in sorted(set(hashlib.algorithms_available) | set(hashlib.algorithms_guaranteed)):
        if _is_blake2(name) or name in keep:
            continue
        try:
            _hashlib.new(name, b"")
        except Exception:
            hashlib.algorithms_guaranteed.discard(name)
            hashlib.algorithms_available.discard(name)

    engaged = True
    kept = tuple(keep)
    removed = tuple(drop)


def _main():
    try:
        import _hashlib
    except Exception:
        return  # No OpenSSL-backed hashlib; nothing to gate.

    try:
        if not _hashlib.get_fips_mode():
            return
    except Exception:
        return  # OpenSSL too old to ask.

    import hashlib

    # Sanity gate: approved mode is claimed, but will OpenSSL actually compute
    # an approved digest? Ask it to, rather than inferring from __module__ --
    # a constructor can report "_hashlib" and still fail when called, because
    # hashlib resolved it with the loose "-fips" query while a real call uses
    # the strict default one. A config where those differ (a default provider
    # active but fips=yes demanded, and no FIPS provider) already breaks stock
    # Python: `hashlib.sha256(b"x")` raises there with no gate installed at
    # all. There is no validated module in play to protect in that state, so
    # restricting hashlib further would only add confusion.
    try:
        _hashlib.new("sha256", b"")
    except Exception as exc:
        sys.stderr.write(
            "memgraph_fips_hashlib: OpenSSL reports approved mode but will not "
            "compute SHA-256 (%s: %s). The OpenSSL configuration is broken; "
            "leaving hashlib untouched.\n" % (type(exc).__name__, exc)
        )
        return

    try:
        _install(_hashlib, hashlib)
    except Exception as exc:  # pragma: no cover
        # Never prevent the interpreter from starting. The verifier asserts the
        # outcome rather than trusting this file to have run.
        sys.stderr.write(
            "memgraph_fips_hashlib: could not restrict hashlib (%r); hashlib "
            "retains its default fallback behaviour\n" % (exc,)
        )
        return

    if os.environ.get("MEMGRAPH_FIPS_HASHLIB_DEBUG"):
        sys.stderr.write(
            "memgraph_fips_hashlib: kept=%s removed=%s\n"
            % (",".join(kept), ",".join(removed) or "-")
        )


_main()
