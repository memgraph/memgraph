#!/usr/bin/env python3
"""Exercise the SAML signature path under OpenSSL approved mode.

Run inside the memgraph container by the smoke suite's
test_fips_saml_signature_path. Exits non-zero, with the reason on stderr, if any
step behaves wrongly.

SAML reaches OpenSSL through xmlsec rather than directly, and that integration
is the part that breaks first in approved mode, so a signature is produced and
verified here for real. All key material is generated in-process: there is no
fixture to expire and no IdP to stand up.

The e2e SSO tests are deliberately not reused. test_saml_sso_module.py
monkey-patches OneLogin_Saml2_Utils.validate_sign to a no-op, so it never
touches crypto, and its captured responses cannot be verified anyway — the
assertion timestamps were hand-edited to 2124 to stop the fixtures expiring,
which invalidates the signatures. Nothing noticed, because validation was
patched out.
"""

import datetime
import sys

AUTH_MODULE_DIR = "/usr/lib/memgraph/auth_module"
sys.path.insert(0, AUTH_MODULE_DIR)

import lxml.etree as ET
import xmlsec
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

DSIG_NS = "{http://www.w3.org/2000/09/xmldsig#}Signature"


def make_key_and_cert():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fips-smoke")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return key_pem, cert.public_bytes(serialization.Encoding.PEM)


def sign(doc, key_pem, sig_alg, digest_alg):
    signature = xmlsec.template.create(doc, xmlsec.Transform.EXCL_C14N, sig_alg)
    doc.append(signature)
    reference = xmlsec.template.add_reference(signature, digest_alg)
    xmlsec.template.add_transform(reference, xmlsec.Transform.ENVELOPED)
    xmlsec.template.ensure_key_info(signature)
    ctx = xmlsec.SignatureContext()
    ctx.key = xmlsec.Key.from_memory(key_pem, xmlsec.KeyFormat.PEM)
    ctx.sign(signature)
    return signature


def check_reference_module():
    """The shipped saml.py turns rejectDeprecatedAlgorithm on from the runtime
    FIPS state rather than at build time, so that one file serves both images.
    Nothing else asserts it, and getting it wrong is silent: SAML would keep
    accepting SHA-1 signatures in approved mode."""
    import saml

    if not saml.fips_approved_mode():
        sys.exit(f"{AUTH_MODULE_DIR}/saml.py does not see approved mode")
    if not saml.SETTINGS_TEMPLATE["security"]["rejectDeprecatedAlgorithm"]:
        sys.exit("saml.py has rejectDeprecatedAlgorithm off in approved mode")
    print("  saml.py: rejectDeprecatedAlgorithm on (SHA-1 assertions refused)")


def main():
    check_reference_module()

    key_pem, cert_pem = make_key_and_cert()
    print("  RSA-2048 keygen and self-signed cert: ok")

    doc = ET.fromstring('<Envelope xmlns="urn:fips:smoke"><Data>signed</Data></Envelope>')
    sign(doc, key_pem, xmlsec.Transform.RSA_SHA256, xmlsec.Transform.SHA256)
    print("  xmlsec sign, RSA-SHA256 over a SHA-256 digest: ok")

    ctx = xmlsec.SignatureContext()
    ctx.key = xmlsec.Key.from_memory(cert_pem, xmlsec.KeyFormat.CERT_PEM)
    ctx.verify(doc.find(f".//{DSIG_NS}"))
    print("  xmlsec verify: ok")

    # SHA-1 is what rejectDeprecatedAlgorithm exists to refuse. Approved mode
    # must refuse it in the signature path too, not only in hashlib -- this is
    # the assertion that makes this a FIPS test rather than a generic one.
    doc2 = ET.fromstring('<E xmlns="urn:x"><D>d</D></E>')
    try:
        sign(doc2, key_pem, xmlsec.Transform.RSA_SHA1, xmlsec.Transform.SHA1)
    except Exception:
        print("  RSA-SHA1 sign: correctly refused")
    else:
        sys.exit("RSA-SHA1 signing succeeded in approved mode")


if __name__ == "__main__":
    main()
