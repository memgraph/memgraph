# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os
import shutil
import subprocess
import sys
import time
from functools import partial

import interactive_mg_runner
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    execute_and_ignore_dead_replica,
    get_data_path,
    get_logs_path,
    show_instances,
    wait_until_main_writeable,
)
from mg_utils import (
    mg_sleep_and_assert,
    mg_sleep_and_assert_collection,
    mg_sleep_and_assert_eval_function,
    mg_sleep_and_assert_multiple,
    mg_sleep_and_assert_until_role_change,
    wait_for_status_change,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "tls_cluster"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description_no_setup(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--management-port=10011",
                "--storage-wal-file-size-kib=1",
                f"--cluster-key-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/instance1.key",
                f"--cluster-cert-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/instance1.crt",
                f"--cluster-ca-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port=7688",
                "--log-level=TRACE",
                "--management-port=10012",
                "--storage-wal-file-size-kib=1",
                f"--cluster-key-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/instance2.key",
                f"--cluster-cert-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/instance2.crt",
                f"--cluster-ca-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port=7689",
                "--log-level=TRACE",
                "--management-port=10013",
                "--storage-wal-file-size-kib=1",
                f"--cluster-key-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/instance3.key",
                f"--cluster-cert-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/instance3.crt",
                f"--cluster-ca-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port=7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname=localhost",
                f"--cluster-key-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/coord1.key",
                f"--cluster-cert-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/coord1.crt",
                f"--cluster-ca-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port=7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname=localhost",
                f"--cluster-key-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/coord2.key",
                f"--cluster-cert-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/coord2.crt",
                f"--cluster-ca-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port=7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname=localhost",
                f"--cluster-key-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/coord3.key",
                f"--cluster-cert-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/coord3.crt",
                f"--cluster-ca-file={interactive_mg_runner.SCRIPT_DIR}/tls_certs/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
    }


# Uses SYNC and ASYNC replicas
def get_sync_cluster():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 AS ASYNC WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]


def setup_cluster(test_name, setup_queries):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)
    return inner_instances_description


def test_tls_cluster_connected(test_name):
    inner_instances_description = setup_cluster(test_name, get_sync_cluster())
    coord1 = connect(host="localhost", port=7690).cursor()
    execute_and_fetch_all(coord1, "show instances")


SCRIPT_TLS_CERTS = os.path.join(interactive_mg_runner.SCRIPT_DIR, "tls_certs")


def _stage_tls_certs(dst_dir: str) -> None:
    """Copy ca + per-instance + per-coord cert/key files into dst_dir so the
    test can rotate them in place without mutating the shared fixture."""
    os.makedirs(dst_dir, exist_ok=True)
    for fname in os.listdir(SCRIPT_TLS_CERTS):
        shutil.copy(os.path.join(SCRIPT_TLS_CERTS, fname), os.path.join(dst_dir, fname))


def _read_cert_serial_from_file(cert_file: str) -> str:
    """openssl x509 -in <cert_file> -serial -noout → uppercase hex string."""
    result = subprocess.run(
        ["openssl", "x509", "-in", cert_file, "-serial", "-noout"],
        capture_output=True,
        text=True,
        check=True,
    )
    out = result.stdout.strip()
    assert out.startswith("serial="), f"unexpected openssl x509 output: {out!r}"
    return out[len("serial=") :]


def _probe_cert_serial(
    host: str,
    port: int,
    client_cert: str,
    client_key: str,
    ca_file: str,
    retries: int = 20,
    retry_sleep: float = 0.5,
) -> str:
    """Open a fresh TLS connection to host:port via openssl s_client (in a
    subprocess), pipe the PEM cert chain into `openssl x509 -serial -noout`,
    return the hex serial of the leaf cert. We deliberately avoid `-quiet`
    because in some OpenSSL builds it implicitly enables `-ign_eof`, which
    keeps s_client running until the outer `timeout` kills it AND suppresses
    the cert output entirely. `echo |` closes stdin cleanly after the handshake."""
    cmd = (
        "echo | timeout 5 openssl s_client "
        f"-connect {host}:{port} -cert {client_cert} -key {client_key} -CAfile {ca_file} "
        "2>/dev/null | openssl x509 -serial -noout 2>/dev/null"
    )
    for _ in range(retries):
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        out = result.stdout.strip()
        if out.startswith("serial="):
            return out[len("serial=") :]
        time.sleep(retry_sleep)
    raise AssertionError(f"Failed to probe cert serial at {host}:{port} after {retries} retries")


def _instances_description_with_tls_dir(test_name: str, tls_dir: str) -> dict:
    """Same shape as get_instances_description_no_setup but every --cluster-*
    path points into tls_dir so the test can rotate the on-disk content."""
    return {
        "instance_1": {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--management-port=10011",
                "--storage-wal-file-size-kib=1",
                f"--cluster-key-file={tls_dir}/instance1.key",
                f"--cluster-cert-file={tls_dir}/instance1.crt",
                f"--cluster-ca-file={tls_dir}/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port=7688",
                "--log-level=TRACE",
                "--management-port=10012",
                "--storage-wal-file-size-kib=1",
                f"--cluster-key-file={tls_dir}/instance2.key",
                f"--cluster-cert-file={tls_dir}/instance2.crt",
                f"--cluster-ca-file={tls_dir}/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port=7689",
                "--log-level=TRACE",
                "--management-port=10013",
                "--storage-wal-file-size-kib=1",
                f"--cluster-key-file={tls_dir}/instance3.key",
                f"--cluster-cert-file={tls_dir}/instance3.crt",
                f"--cluster-ca-file={tls_dir}/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port=7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname=localhost",
                f"--cluster-key-file={tls_dir}/coord1.key",
                f"--cluster-cert-file={tls_dir}/coord1.crt",
                f"--cluster-ca-file={tls_dir}/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port=7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname=localhost",
                f"--cluster-key-file={tls_dir}/coord2.key",
                f"--cluster-cert-file={tls_dir}/coord2.crt",
                f"--cluster-ca-file={tls_dir}/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port=7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname=localhost",
                f"--cluster-key-file={tls_dir}/coord3.key",
                f"--cluster-cert-file={tls_dir}/coord3.crt",
                f"--cluster-ca-file={tls_dir}/ca.crt",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
    }


def test_reload_intra_cluster_tls(test_name, tmp_path):
    """Verify that `RELOAD INTRA_CLUSTER TLS` swaps the cert served by:
      * each data instance's management server (10011-10013),
      * each data instance's replication server (10001-10002 for replicas),
      * each coordinator's management server (10121-10123),
      * each coordinator's NuRaft server (10111-10113),
    without a process restart.

    Strategy:
      1. Stage a writable temp copy of tls_certs/ so we can rotate in place.
      2. Boot the cluster (instance_3 is MAIN; instance_1, instance_2 are replicas).
      3. Probe each port's served cert serial via `openssl s_client`.
      4. Cyclically overwrite cert+key pairs on disk:
           instance1's files ← instance2's content, instance2 ← instance3, instance3 ← instance1,
           coord1's files    ← coord2's content,    coord2    ← coord3,    coord3    ← coord1.
      5. Execute `RELOAD INTRA_CLUSTER TLS` on each data instance AND each coordinator.
      6. Re-probe and assert each served serial now matches the rotated source
         (proves the reload actually picked up the new file) and differs from
         the initial serial (catches a no-op reload).
    """
    tls_dir = str(tmp_path / "tls_certs")
    _stage_tls_certs(tls_dir)

    inner_instances_description = _instances_description_with_tls_dir(test_name, tls_dir)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in get_sync_cluster():
        execute_and_fetch_all(coord_cursor_3, query)

    ca_file = os.path.join(tls_dir, "ca.crt")
    # Probe client identity: read from the read-only fixture dir (never
    # rotated) so a single (cert, key) pair stays valid for every probe.
    # We use instance1's pair — also signed by ca.crt and never mutated here.
    probe_client_cert = os.path.join(SCRIPT_TLS_CERTS, "instance1.crt")
    probe_client_key = os.path.join(SCRIPT_TLS_CERTS, "instance1.key")

    # 1) Pre-reload serials.
    initial = {
        "instance1_mgmt": _probe_cert_serial("127.0.0.1", 10011, probe_client_cert, probe_client_key, ca_file),
        "instance2_mgmt": _probe_cert_serial("127.0.0.1", 10012, probe_client_cert, probe_client_key, ca_file),
        "instance3_mgmt": _probe_cert_serial("127.0.0.1", 10013, probe_client_cert, probe_client_key, ca_file),
        "instance1_repl": _probe_cert_serial("127.0.0.1", 10001, probe_client_cert, probe_client_key, ca_file),
        "instance2_repl": _probe_cert_serial("127.0.0.1", 10002, probe_client_cert, probe_client_key, ca_file),
        "coord1_mgmt": _probe_cert_serial("127.0.0.1", 10121, probe_client_cert, probe_client_key, ca_file),
        "coord2_mgmt": _probe_cert_serial("127.0.0.1", 10122, probe_client_cert, probe_client_key, ca_file),
        "coord3_mgmt": _probe_cert_serial("127.0.0.1", 10123, probe_client_cert, probe_client_key, ca_file),
        "coord1_raft": _probe_cert_serial("127.0.0.1", 10111, probe_client_cert, probe_client_key, ca_file),
        "coord2_raft": _probe_cert_serial("127.0.0.1", 10112, probe_client_cert, probe_client_key, ca_file),
        "coord3_raft": _probe_cert_serial("127.0.0.1", 10113, probe_client_cert, probe_client_key, ca_file),
    }

    # Sanity: every port serves its own cert pre-reload.
    instance_serials = {
        n: _read_cert_serial_from_file(os.path.join(SCRIPT_TLS_CERTS, f"instance{n}.crt")) for n in (1, 2, 3)
    }
    coord_serials = {n: _read_cert_serial_from_file(os.path.join(SCRIPT_TLS_CERTS, f"coord{n}.crt")) for n in (1, 2, 3)}

    assert initial["instance1_mgmt"] == instance_serials[1]
    assert initial["instance2_mgmt"] == instance_serials[2]
    assert initial["instance3_mgmt"] == instance_serials[3]
    assert initial["instance1_repl"] == instance_serials[1]
    assert initial["instance2_repl"] == instance_serials[2]
    assert initial["coord1_mgmt"] == coord_serials[1]
    assert initial["coord2_mgmt"] == coord_serials[2]
    assert initial["coord3_mgmt"] == coord_serials[3]
    assert initial["coord1_raft"] == coord_serials[1]
    assert initial["coord2_raft"] == coord_serials[2]
    assert initial["coord3_raft"] == coord_serials[3]

    # 2) Rotate certs+keys on disk cyclically. After this:
    #    tls_dir/instanceN.{crt,key} contains the content of instance((N % 3)+1)
    #    tls_dir/coordN.{crt,key}    contains the content of coord((N % 3)+1)
    rotation = [(1, 2), (2, 3), (3, 1)]  # (target_file_idx, source_content_idx)
    for prefix in ("instance", "coord"):
        for target, source in rotation:
            shutil.copy(
                os.path.join(SCRIPT_TLS_CERTS, f"{prefix}{source}.crt"), os.path.join(tls_dir, f"{prefix}{target}.crt")
            )
            shutil.copy(
                os.path.join(SCRIPT_TLS_CERTS, f"{prefix}{source}.key"), os.path.join(tls_dir, f"{prefix}{target}.key")
            )

    expected_after = {
        "instance1": instance_serials[2],
        "instance2": instance_serials[3],
        "instance3": instance_serials[1],
        "coord1": coord_serials[2],
        "coord2": coord_serials[3],
        "coord3": coord_serials[1],
    }

    # 3) Trigger reload on each data instance AND each coordinator via its Bolt port.
    for bolt_port in (7687, 7688, 7689, 7690, 7691, 7692):
        cursor = connect(host="localhost", port=bolt_port).cursor()
        execute_and_fetch_all(cursor, "RELOAD INTRA_CLUSTER TLS")

    # 4) Post-reload serials.
    after = {
        "instance1_mgmt": _probe_cert_serial("127.0.0.1", 10011, probe_client_cert, probe_client_key, ca_file),
        "instance2_mgmt": _probe_cert_serial("127.0.0.1", 10012, probe_client_cert, probe_client_key, ca_file),
        "instance3_mgmt": _probe_cert_serial("127.0.0.1", 10013, probe_client_cert, probe_client_key, ca_file),
        "instance1_repl": _probe_cert_serial("127.0.0.1", 10001, probe_client_cert, probe_client_key, ca_file),
        "instance2_repl": _probe_cert_serial("127.0.0.1", 10002, probe_client_cert, probe_client_key, ca_file),
        "coord1_mgmt": _probe_cert_serial("127.0.0.1", 10121, probe_client_cert, probe_client_key, ca_file),
        "coord2_mgmt": _probe_cert_serial("127.0.0.1", 10122, probe_client_cert, probe_client_key, ca_file),
        "coord3_mgmt": _probe_cert_serial("127.0.0.1", 10123, probe_client_cert, probe_client_key, ca_file),
        "coord1_raft": _probe_cert_serial("127.0.0.1", 10111, probe_client_cert, probe_client_key, ca_file),
        "coord2_raft": _probe_cert_serial("127.0.0.1", 10112, probe_client_cert, probe_client_key, ca_file),
        "coord3_raft": _probe_cert_serial("127.0.0.1", 10113, probe_client_cert, probe_client_key, ca_file),
    }

    # Data-instance management server reload (DataInstanceManagementServer::ReloadTls).
    assert after["instance1_mgmt"] == expected_after["instance1"], (
        f"instance_1 management server did not pick up rotated cert. "
        f"before={initial['instance1_mgmt']} after={after['instance1_mgmt']} expected={expected_after['instance1']}"
    )
    assert after["instance2_mgmt"] == expected_after["instance2"]
    assert after["instance3_mgmt"] == expected_after["instance3"]
    assert after["instance1_mgmt"] != initial["instance1_mgmt"]
    assert after["instance2_mgmt"] != initial["instance2_mgmt"]
    assert after["instance3_mgmt"] != initial["instance3_mgmt"]

    # Replication server reload (ReplicationServer::ReloadTls). Only replicas
    # have a listening replication server; instance_3 is MAIN so no port-10003 check.
    assert after["instance1_repl"] == expected_after["instance1"], (
        f"instance_1 replication server did not pick up rotated cert. "
        f"before={initial['instance1_repl']} after={after['instance1_repl']} expected={expected_after['instance1']}"
    )
    assert after["instance2_repl"] == expected_after["instance2"]
    assert after["instance1_repl"] != initial["instance1_repl"]
    assert after["instance2_repl"] != initial["instance2_repl"]

    # Coordinator management server reload (CoordinatorInstanceManagementServer::ReloadTls,
    # invoked via CoordinatorInstance::ReloadTls).
    assert after["coord1_mgmt"] == expected_after["coord1"], (
        f"coordinator_1 management server did not pick up rotated cert. "
        f"before={initial['coord1_mgmt']} after={after['coord1_mgmt']} expected={expected_after['coord1']}"
    )
    assert after["coord2_mgmt"] == expected_after["coord2"]
    assert after["coord3_mgmt"] == expected_after["coord3"]
    assert after["coord1_mgmt"] != initial["coord1_mgmt"]
    assert after["coord2_mgmt"] != initial["coord2_mgmt"]
    assert after["coord3_mgmt"] != initial["coord3_mgmt"]

    # Coordinator NuRaft server reload (the asio_service SSL contexts driving the
    # Raft listener on --coordinator-port). Same --cluster-cert-file as the mgmt
    # server, so the expected serial after rotation is the rotated coord cert.
    assert after["coord1_raft"] == expected_after["coord1"], (
        f"coordinator_1 NuRaft server did not pick up rotated cert. "
        f"before={initial['coord1_raft']} after={after['coord1_raft']} expected={expected_after['coord1']}"
    )
    assert after["coord2_raft"] == expected_after["coord2"]
    assert after["coord3_raft"] == expected_after["coord3"]
    assert after["coord1_raft"] != initial["coord1_raft"]
    assert after["coord2_raft"] != initial["coord2_raft"]
    assert after["coord3_raft"] != initial["coord3_raft"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
