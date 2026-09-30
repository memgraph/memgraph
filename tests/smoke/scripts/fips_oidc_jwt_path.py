#!/usr/bin/env python3
"""Exercise the OIDC token path under OpenSSL approved mode.

Run inside the memgraph container by the smoke suite's
test_fips_oidc_jwt_path. Exits non-zero, with the reason on stderr, if any step
behaves wrongly.

OIDC reaches OpenSSL through PyJWT and cryptography rather than directly. RS256
is the only algorithm src/auth/reference_modules/oidc.py accepts, so that is
what is exercised, with the key generated in-process.
"""

import _hashlib
import sys

import jwt
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

AUDIENCE = "mg"


def main():
    # Without this the rest proves nothing: every assertion below passes just as
    # well on a non-FIPS build, so the run has to establish that OpenSSL really
    # is in approved mode before claiming anything about it.
    if not _hashlib.get_fips_mode():
        sys.exit("OpenSSL is not in approved mode, so this test proves nothing")
    print("  OpenSSL reports approved mode")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    public_pem = key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    token = jwt.encode({"sub": "fips", "aud": AUDIENCE}, private_pem, algorithm="RS256")
    claims = jwt.decode(token, public_pem, algorithms=["RS256"], audience=AUDIENCE)
    if claims["sub"] != "fips":
        sys.exit(f"RS256 round trip returned the wrong subject: {claims.get('sub')!r}")
    print("  PyJWT RS256 sign and verify: ok")

    try:
        jwt.decode(token, public_pem, algorithms=["RS256"], audience="not-" + AUDIENCE)
    except jwt.InvalidAudienceError:
        print("  audience rejection: ok")
    else:
        sys.exit("a wrong audience was accepted")

    # The FIPS-specific half. An IdP key below the approved floor, and a SHA-1
    # signature, must both be refused by the provider rather than merely
    # discouraged -- these are what fail on a non-FIPS build and so are what
    # make this a FIPS test.
    weak = rsa.generate_private_key(public_exponent=65537, key_size=1024)
    try:
        weak.sign(b"m", padding.PKCS1v15(), hashes.SHA256())
    except Exception:
        print("  RSA-1024 signing: correctly refused")
    else:
        sys.exit("RSA-1024 signing succeeded in approved mode")

    try:
        key.sign(b"m", padding.PKCS1v15(), hashes.SHA1())
    except Exception:
        print("  SHA-1 signing: correctly refused")
    else:
        sys.exit("SHA-1 signing succeeded in approved mode")


if __name__ == "__main__":
    main()
