#!/usr/bin/env python3
"""Exercise the OIDC token path under OpenSSL approved mode.

Run inside the memgraph container by the smoke suite's
test_fips_oidc_jwt_path. Exits non-zero, with the reason on stderr, if any step
behaves wrongly.

OIDC reaches OpenSSL through PyJWT and cryptography rather than directly. RS256
is the only algorithm src/auth/reference_modules/oidc.py accepts, so that is
what is exercised, with the key generated in-process.
"""

import sys

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

AUDIENCE = "mg"


def main():
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


if __name__ == "__main__":
    main()
