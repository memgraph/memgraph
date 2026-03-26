# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import hashlib
import os
import socket
import ssl
import subprocess
import sys

import pytest
from common import connect_ssl, execute_and_fetch_all

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CERT_FILE = os.path.join(SCRIPT_DIR, "ssl_reload_test.crt")
KEY_FILE = os.path.join(SCRIPT_DIR, "ssl_reload_test.key")


def generate_self_signed_cert(cert_path, key_path, cn="test-cert"):
    """Generate a self-signed certificate with a given CN."""
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key_path,
            "-out",
            cert_path,
            "-days",
            "1",
            "-nodes",
            "-subj",
            f"/CN={cn}",
        ],
        check=True,
        capture_output=True,
    )


def get_server_cert_fingerprint(host="127.0.0.1", port=7687):
    """Connect via raw SSL and return the SHA256 fingerprint of the server certificate."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=5) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            der_cert = ssock.getpeercert(binary_form=True)
            return hashlib.sha256(der_cert).hexdigest()


def test_ssl_hot_reload():
    """
    Test that SSL certificates can be reloaded at runtime.

    Memgraph is already running with ssl_reload_test.crt/key (started by the runner).
    We overwrite the cert/key files, call RELOAD SSL BOLT_SERVER, and verify
    new connections use the new certificate while old connections still work.
    """
    # Step 1: Connect and record the initial certificate fingerprint
    initial_fingerprint = get_server_cert_fingerprint()
    old_conn = connect_ssl()
    execute_and_fetch_all(old_conn.cursor(), "RETURN 1 AS n")

    # Step 2: Overwrite the cert/key files with a new certificate
    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="reloaded-cert")

    # Step 3: Reload SSL context
    cursor = old_conn.cursor()
    execute_and_fetch_all(cursor, "RELOAD SSL BOLT_SERVER;")

    # Step 4: New connection should see the new certificate
    new_fingerprint = get_server_cert_fingerprint()
    assert new_fingerprint != initial_fingerprint, (
        "New connections should use the new certificate after reload, "
        f"but fingerprint is still {initial_fingerprint}"
    )

    new_conn = connect_ssl()
    execute_and_fetch_all(new_conn.cursor(), "RETURN 1 AS n")
    new_conn.close()

    # Step 5: Old connection should still work
    result = execute_and_fetch_all(old_conn.cursor(), "RETURN 2 AS n")
    assert result == [(2,)], f"Old connection should still work after reload, got: {result}"
    old_conn.close()


def test_ssl_hot_reload_invalid_cert_keeps_old():
    """
    Test that reloading with an invalid certificate gracefully fails
    and keeps serving with the old certificate.
    """
    # Restore a valid cert first (previous test may have changed it)
    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="valid-cert")
    cursor = connect_ssl().cursor()
    execute_and_fetch_all(cursor, "RELOAD SSL BOLT_SERVER;")

    original_fingerprint = get_server_cert_fingerprint()

    # Overwrite cert with garbage
    with open(CERT_FILE, "w") as f:
        f.write("this is not a valid certificate")
    with open(KEY_FILE, "w") as f:
        f.write("this is not a valid key")

    # Reload should fail gracefully
    try:
        execute_and_fetch_all(cursor, "RELOAD SSL BOLT_SERVER;")
    except Exception:
        pass  # Expected to fail or return error

    # New connections should still work with the old cert
    still_same_fingerprint = get_server_cert_fingerprint()
    assert (
        still_same_fingerprint == original_fingerprint
    ), "After failed reload, server should still use the old certificate"
    verify_conn = connect_ssl()
    execute_and_fetch_all(verify_conn.cursor(), "RETURN 1 AS n")
    verify_conn.close()

    # Restore the original cert for any subsequent tests
    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="restored-cert")
    execute_and_fetch_all(cursor, "RELOAD SSL BOLT_SERVER;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
