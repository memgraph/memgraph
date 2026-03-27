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
import threading

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


def get_server_cert_cn(host="127.0.0.1", port=7687):
    """Connect via raw SSL and return the CN of the server certificate."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=5) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            cert = ssock.getpeercert()
            if cert:
                for rdn in cert.get("subject", ()):
                    for attr_type, attr_value in rdn:
                        if attr_type == "commonName":
                            return attr_value
            # Fallback: parse DER cert to extract CN via openssl
            der_cert = ssock.getpeercert(binary_form=True)
            result = subprocess.run(
                ["openssl", "x509", "-noout", "-subject", "-inform", "DER"],
                input=der_cert,
                capture_output=True,
            )
            # Output like: subject=CN = some-cn
            line = result.stdout.decode().strip()
            if "CN" in line:
                return line.split("CN")[-1].strip().lstrip("=").strip()
            return None


def reload_ssl(conn=None):
    """Execute RELOAD BOLT_SERVER TLS on a connection. Opens a new one if none provided."""
    if conn is None:
        conn = connect_ssl()
    execute_and_fetch_all(conn.cursor(), "RELOAD BOLT_SERVER TLS;")
    return conn


def ensure_clean_state():
    """Restore a known-good cert and reload so tests start from a clean state."""
    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="clean-state")
    reload_ssl()


# ---------------------------------------------------------------------------
# Test 1: Basic hot reload
# ---------------------------------------------------------------------------
def test_ssl_hot_reload():
    """
    Overwrite cert files, reload, verify new connections get the new cert
    and old connections still work.
    """
    initial_fingerprint = get_server_cert_fingerprint()
    old_conn = connect_ssl()
    execute_and_fetch_all(old_conn.cursor(), "RETURN 1 AS n")

    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="reloaded-cert")
    reload_ssl(old_conn)

    new_fingerprint = get_server_cert_fingerprint()
    assert new_fingerprint != initial_fingerprint, (
        "New connections should use the new certificate after reload, "
        f"but fingerprint is still {initial_fingerprint}"
    )

    new_conn = connect_ssl()
    execute_and_fetch_all(new_conn.cursor(), "RETURN 1 AS n")
    new_conn.close()

    result = execute_and_fetch_all(old_conn.cursor(), "RETURN 2 AS n")
    assert result == [(2,)], f"Old connection should still work after reload, got: {result}"
    old_conn.close()


# ---------------------------------------------------------------------------
# Test 2: Multiple rapid reloads
# ---------------------------------------------------------------------------
def test_ssl_multiple_rapid_reloads():
    """
    Rotate certs several times in quick succession.
    Each reload should take effect for new connections.
    """
    fingerprints = set()

    for i in range(5):
        generate_self_signed_cert(CERT_FILE, KEY_FILE, cn=f"rapid-reload-{i}")
        reload_ssl()
        fp = get_server_cert_fingerprint()
        fingerprints.add(fp)

        # Verify connections still work after each reload
        conn = connect_ssl()
        result = execute_and_fetch_all(conn.cursor(), "RETURN 1 AS n")
        assert result == [(1,)], f"Connection failed after reload iteration {i}"
        conn.close()

    # All 5 rotations should have produced different fingerprints
    assert len(fingerprints) == 5, f"Expected 5 distinct certificate fingerprints, got {len(fingerprints)}"


# ---------------------------------------------------------------------------
# Test 3: Concurrent connections during reload
# ---------------------------------------------------------------------------
def test_ssl_concurrent_connections_during_reload():
    """
    Open multiple connections, reload while they're active,
    verify all old connections still work and new ones get the new cert.
    """
    # Open several connections before reload
    old_connections = []
    for _ in range(5):
        conn = connect_ssl()
        execute_and_fetch_all(conn.cursor(), "RETURN 1 AS n")
        old_connections.append(conn)

    initial_fingerprint = get_server_cert_fingerprint()

    # Reload with a new cert
    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="concurrent-reload")
    reload_ssl(old_connections[0])

    # All old connections should still work
    for i, conn in enumerate(old_connections):
        result = execute_and_fetch_all(conn.cursor(), f"RETURN {i} AS n")
        assert result == [(i,)], f"Old connection {i} broken after reload"

    # New connections should see the new cert
    new_fingerprint = get_server_cert_fingerprint()
    assert new_fingerprint != initial_fingerprint, "New connections should use the new certificate"

    new_conn = connect_ssl()
    execute_and_fetch_all(new_conn.cursor(), "RETURN 1 AS n")
    new_conn.close()

    for conn in old_connections:
        conn.close()


# ---------------------------------------------------------------------------
# Test 4: Reload under load
# ---------------------------------------------------------------------------
def test_ssl_reload_under_load():
    """
    Run queries on one connection while another triggers a reload.
    Neither should fail.
    """
    query_conn = connect_ssl()
    reload_conn = connect_ssl()

    errors = []

    def run_queries():
        try:
            for i in range(50):
                result = execute_and_fetch_all(query_conn.cursor(), f"RETURN {i} AS n")
                assert result == [(i,)], f"Query {i} returned unexpected result: {result}"
        except Exception as e:
            errors.append(f"Query thread error: {e}")

    def do_reload():
        try:
            generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="under-load-cert")
            reload_ssl(reload_conn)
        except Exception as e:
            errors.append(f"Reload thread error: {e}")

    query_thread = threading.Thread(target=run_queries)
    reload_thread = threading.Thread(target=do_reload)

    query_thread.start()
    reload_thread.start()

    query_thread.join(timeout=30)
    reload_thread.join(timeout=30)

    assert not errors, f"Errors during reload under load: {errors}"

    query_conn.close()
    reload_conn.close()


# ---------------------------------------------------------------------------
# Test 5: Certificate CN matches after reload
# ---------------------------------------------------------------------------
def test_ssl_cert_cn_matches_after_reload():
    """
    After reload, verify the served certificate's CN matches the newly generated cert.
    """
    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="cn-check-first")
    reload_ssl()

    cn = get_server_cert_cn()
    assert cn == "cn-check-first", f"Expected CN 'cn-check-first', got '{cn}'"

    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="cn-check-second")
    reload_ssl()

    cn = get_server_cert_cn()
    assert cn == "cn-check-second", f"Expected CN 'cn-check-second', got '{cn}'"


# ---------------------------------------------------------------------------
# Test 6: Invalid cert then valid cert (recovery)
# ---------------------------------------------------------------------------
def test_ssl_hot_reload_invalid_cert_keeps_old():
    """
    Reload with an invalid certificate, verify old cert is still served,
    then reload with a new valid cert and verify it takes effect.
    """
    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="valid-before-bad")
    conn = connect_ssl()
    reload_ssl(conn)

    original_fingerprint = get_server_cert_fingerprint()

    # Overwrite cert with garbage
    with open(CERT_FILE, "w") as f:
        f.write("this is not a valid certificate")
    with open(KEY_FILE, "w") as f:
        f.write("this is not a valid key")

    # Reload should fail gracefully
    try:
        execute_and_fetch_all(conn.cursor(), "RELOAD BOLT_SERVER TLS;")
    except Exception:
        pass  # Expected to fail or return error

    # Old cert should still be served
    still_same_fingerprint = get_server_cert_fingerprint()
    assert (
        still_same_fingerprint == original_fingerprint
    ), "After failed reload, server should still use the old certificate"
    verify_conn = connect_ssl()
    execute_and_fetch_all(verify_conn.cursor(), "RETURN 1 AS n")
    verify_conn.close()

    # Now restore with a new valid cert — should recover
    generate_self_signed_cert(CERT_FILE, KEY_FILE, cn="valid-after-bad")
    reload_ssl(conn)

    recovered_fingerprint = get_server_cert_fingerprint()
    assert recovered_fingerprint != original_fingerprint, "After recovery reload, server should use the new certificate"

    recovery_conn = connect_ssl()
    execute_and_fetch_all(recovery_conn.cursor(), "RETURN 1 AS n")
    recovery_conn.close()
    conn.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
