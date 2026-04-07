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

import interactive_mg_runner
import mgclient
import pytest
from common import get_data_path, get_logs_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "test_ssl_hot_reload_coordinator"

# Per-coordinator cert/key paths (copied to build dir by CMake)
COORD_CERT_FILES = {}
COORD_KEY_FILES = {}
for i in range(1, 4):
    COORD_CERT_FILES[i] = os.path.join(interactive_mg_runner.BUILD_DIR, f"ssl_coordinator_{i}.crt")
    COORD_KEY_FILES[i] = os.path.join(interactive_mg_runner.BUILD_DIR, f"ssl_coordinator_{i}.key")

COORD_PORTS = {1: 7690, 2: 7691, 3: 7692}


def generate_self_signed_cert(cert_path, key_path, cn="test-cert"):
    """Generate a self-signed certificate with a given CN."""
    os.makedirs(os.path.dirname(cert_path), exist_ok=True)
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


def get_server_cert_fingerprint(host="127.0.0.1", port=7690):
    """Connect via raw SSL and return the SHA256 fingerprint of the server certificate."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=5) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            der_cert = ssock.getpeercert(binary_form=True)
            return hashlib.sha256(der_cert).hexdigest()


def get_server_cert_cn(host="127.0.0.1", port=7690):
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
            der_cert = ssock.getpeercert(binary_form=True)
            result = subprocess.run(
                ["openssl", "x509", "-noout", "-subject", "-inform", "DER"],
                input=der_cert,
                capture_output=True,
            )
            line = result.stdout.decode().strip()
            if "CN" in line:
                return line.split("CN")[-1].strip().lstrip("=").strip()
            return None


def connect_ssl(port=7690):
    """Connect to a coordinator via SSL."""
    connection = mgclient.connect(host="localhost", port=port, sslmode=mgclient.MG_SSLMODE_REQUIRE)
    connection.autocommit = True
    return connection


def execute_and_fetch_all(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def reload_ssl(conn=None, port=7690):
    """Execute RELOAD BOLT_SERVER TLS on a connection. Opens a new one if none provided."""
    if conn is None:
        conn = connect_ssl(port)
    execute_and_fetch_all(conn.cursor(), "RELOAD BOLT_SERVER TLS;")
    return conn


def get_instances_description(test_name: str):
    """Return coordinator cluster description with SSL enabled."""
    return {
        "coordinator_1": {
            "args": [
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname",
                "localhost",
                "--bolt-cert-file",
                COORD_CERT_FILES[1],
                "--bolt-key-file",
                COORD_KEY_FILES[1],
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "ssl": True,
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname",
                "localhost",
                "--bolt-cert-file",
                COORD_CERT_FILES[2],
                "--bolt-key-file",
                COORD_KEY_FILES[2],
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "ssl": True,
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname",
                "localhost",
                "--bolt-cert-file",
                COORD_CERT_FILES[3],
                "--bolt-key-file",
                COORD_KEY_FILES[3],
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "ssl": True,
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
                "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
            ],
        },
    }


@pytest.fixture(autouse=True)
def setup_and_teardown(request):
    """Start coordinator cluster before each test, stop after."""
    test_name = request.node.name

    # Generate initial certs for all coordinators
    for i in range(1, 4):
        generate_self_signed_cert(COORD_CERT_FILES[i], COORD_KEY_FILES[i], cn=f"coordinator-{i}-initial")

    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    yield

    interactive_mg_runner.stop_all(keep_directories=False)


# ---------------------------------------------------------------------------
# Test 1: Basic hot reload on each coordinator
# ---------------------------------------------------------------------------
def test_ssl_hot_reload_coordinator():
    """
    Overwrite cert files on each coordinator, reload, verify new connections
    get the new cert and old connections still work.
    """
    for coord_id, port in COORD_PORTS.items():
        initial_fingerprint = get_server_cert_fingerprint(port=port)
        old_conn = connect_ssl(port)
        execute_and_fetch_all(old_conn.cursor(), "show instance")

        generate_self_signed_cert(
            COORD_CERT_FILES[coord_id], COORD_KEY_FILES[coord_id], cn=f"reloaded-coord-{coord_id}"
        )
        reload_ssl(old_conn, port)

        new_fingerprint = get_server_cert_fingerprint(port=port)
        assert new_fingerprint != initial_fingerprint, (
            f"Coordinator {coord_id}: new connections should use the new certificate after reload, "
            f"but fingerprint is still {initial_fingerprint}"
        )

        new_conn = connect_ssl(port)
        execute_and_fetch_all(new_conn.cursor(), "show instance")
        new_conn.close()

        execute_and_fetch_all(old_conn.cursor(), "show instance")
        old_conn.close()


# ---------------------------------------------------------------------------
# Test 2: Concurrent connections during reload on coordinator
# ---------------------------------------------------------------------------
def test_ssl_concurrent_connections_during_reload_coordinator():
    """
    Open multiple connections to a coordinator, reload while they're active,
    verify all old connections still work and new ones get the new cert.
    """
    for coord_id, port in COORD_PORTS.items():
        old_connections = []
        for _ in range(5):
            conn = connect_ssl(port)
            execute_and_fetch_all(conn.cursor(), "show instance")
            old_connections.append(conn)

        initial_fingerprint = get_server_cert_fingerprint(port=port)

        generate_self_signed_cert(
            COORD_CERT_FILES[coord_id], COORD_KEY_FILES[coord_id], cn=f"concurrent-reload-coord{coord_id}"
        )
        reload_ssl(old_connections[0], port)

        for i, conn in enumerate(old_connections):
            execute_and_fetch_all(conn.cursor(), "show instance")

        new_fingerprint = get_server_cert_fingerprint(port=port)
        assert (
            new_fingerprint != initial_fingerprint
        ), f"Coordinator {coord_id}: new connections should use the new certificate"

        new_conn = connect_ssl(port)
        execute_and_fetch_all(new_conn.cursor(), "show instance")
        new_conn.close()

        for conn in old_connections:
            conn.close()


# ---------------------------------------------------------------------------
# Test 3: Invalid cert then valid cert (recovery) on coordinator
# ---------------------------------------------------------------------------
def test_ssl_hot_reload_invalid_cert_keeps_old_coordinator():
    """
    Reload with an invalid certificate on each coordinator, verify old cert
    is still served, then reload with a new valid cert and verify it takes effect.
    """
    for coord_id, port in COORD_PORTS.items():
        generate_self_signed_cert(
            COORD_CERT_FILES[coord_id], COORD_KEY_FILES[coord_id], cn=f"valid-before-bad-{coord_id}"
        )
        conn = connect_ssl(port)
        reload_ssl(conn, port)

        original_fingerprint = get_server_cert_fingerprint(port=port)

        # Overwrite cert with garbage
        with open(COORD_CERT_FILES[coord_id], "w") as f:
            f.write("this is not a valid certificate")
        with open(COORD_KEY_FILES[coord_id], "w") as f:
            f.write("this is not a valid key")

        # Reload should fail gracefully
        try:
            execute_and_fetch_all(conn.cursor(), "RELOAD BOLT_SERVER TLS;")
        except Exception:
            pass  # Expected to fail or return error

        # Old cert should still be served
        still_same_fingerprint = get_server_cert_fingerprint(port=port)
        assert (
            still_same_fingerprint == original_fingerprint
        ), f"Coordinator {coord_id}: after failed reload, server should still use the old certificate"
        verify_conn = connect_ssl(port)
        execute_and_fetch_all(verify_conn.cursor(), "show instance")
        verify_conn.close()

        # Now restore with a new valid cert - should recover
        generate_self_signed_cert(
            COORD_CERT_FILES[coord_id], COORD_KEY_FILES[coord_id], cn=f"valid-after-bad-{coord_id}"
        )
        reload_ssl(conn, port)

        recovered_fingerprint = get_server_cert_fingerprint(port=port)
        assert (
            recovered_fingerprint != original_fingerprint
        ), f"Coordinator {coord_id}: after recovery reload, server should use the new certificate"

        recovery_conn = connect_ssl(port)
        execute_and_fetch_all(recovery_conn.cursor(), "show instance")
        recovery_conn.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
