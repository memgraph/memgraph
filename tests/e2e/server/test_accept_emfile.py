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

"""
Accept loops must survive descriptor exhaustion (EMFILE) without spinning and must resume accepting once
descriptors are available again. Memgraph is spawned with a small RLIMIT_NOFILE, squeezed to the limit with idle
Bolt connections, held there briefly, then released.
"""

import glob
import os
import re
import resource
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

import interactive_mg_runner
import mgclient
import pytest

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FD_LIMIT = 400
HOLD_SECONDS = 2.5
# A spinning accept loop burns ~HOLD_SECONDS of CPU on one thread; an idle thread burns ~0.
MAX_THREAD_CPU_SECONDS = 0.5
# Time at the fd limit is < one throttle window (10 s), so each throttled listener logs at most once: Bolt, HTTP
# (metrics), WebSocket, RPC.
MAX_ACCEPT_LOG_LINES = 4
ACCEPT_FAILED_RE = re.compile(r"accept failed \(.*\); retrying every \d+ms")
RECOVERY_DEADLINE_SECONDS = 5.0
CLK_TCK = os.sysconf("SC_CLK_TCK")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def fd_count(pid):
    return len(os.listdir(f"/proc/{pid}/fd"))


def thread_cpu_seconds(pid):
    result = {}
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{tid}/stat") as f:
                stat = f.read()
        except FileNotFoundError:
            continue
        comm = stat[stat.index("(") + 1 : stat.rindex(")")]
        fields = stat[stat.rindex(")") + 2 :].split()
        # fields[0] is state (field 3); utime/stime are fields 14/15.
        result[tid] = (comm, (int(fields[11]) + int(fields[12])) / CLK_TCK)
    return result


def retry(fn, deadline_seconds=RECOVERY_DEADLINE_SECONDS):
    deadline = time.time() + deadline_seconds
    last = None
    while time.time() < deadline:
        try:
            if fn():
                return True
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(0.1)
    if last:
        print(f"last probe error: {last!r}")
    return False


def bolt_works(port):
    conn = mgclient.connect(host="127.0.0.1", port=port)
    try:
        cursor = conn.cursor()
        cursor.execute("RETURN 1")
        return cursor.fetchall() == [(1,)]
    finally:
        conn.close()


def ws_handshake_works(port):
    with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
        s.sendall(
            b"GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        )
        return b" 101 " in s.recv(4096).split(b"\r\n", 1)[0] + b" "


def metrics_ok(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as r:
        return r.status == 200


def accept_log_lines(log_path):
    with open(log_path, errors="replace") as f:
        return sum(1 for line in f if ACCEPT_FAILED_RE.search(line))


def accepted_connection_lines(log_path):
    with open(log_path, errors="replace") as f:
        return sum(1 for line in f if re.search(r"Accepted a .* connection", line))


@pytest.fixture(scope="module")
def scenario():
    ports = {name: free_port() for name in ("bolt", "ws", "metrics", "rpc")}
    tmp = tempfile.mkdtemp(prefix="accept_emfile_")

    def limit_fds():
        resource.setrlimit(resource.RLIMIT_NOFILE, (FD_LIMIT, FD_LIMIT))

    proc = subprocess.Popen(
        [
            interactive_mg_runner.MEMGRAPH_BINARY,
            f"--bolt-port={ports['bolt']}",
            f"--monitoring-port={ports['ws']}",
            f"--metrics-port={ports['metrics']}",
            f"--data-directory={os.path.join(tmp, 'data')}",
            f"--log-file={os.path.join(tmp, 'memgraph.log')}",
            "--log-level=INFO",
            "--also-log-to-stderr=false",
            "--storage-snapshot-on-exit=false",
            "--telemetry-enabled=false",
        ],
        preexec_fn=limit_fds,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    pokes = []
    try:
        assert retry(lambda: bolt_works(ports["bolt"]), 60), "memgraph did not start"
        # The log file name gets a date suffix (memgraph_YYYY-MM-DD.log).
        log_path = glob.glob(os.path.join(tmp, "memgraph*.log"))[0]

        conn = mgclient.connect(host="127.0.0.1", port=ports["bolt"])
        conn.autocommit = True
        conn.cursor().execute(f"SET REPLICATION ROLE TO REPLICA WITH PORT {ports['rpc']};")
        conn.close()

        have_metrics = retry(lambda: metrics_ok(ports["metrics"]), 3)

        baseline = fd_count(proc.pid)
        if baseline > FD_LIMIT - 20:
            pytest.skip(f"baseline fd count {baseline} too close to the limit {FD_LIMIT}")

        # Stop one short of the limit: the next accept() on a listener then fails with EMFILE. Also stop if the
        # server stops accepting (connect times out).
        deadline = time.time() + 30
        while fd_count(proc.pid) < FD_LIMIT - 1 and time.time() < deadline:
            try:
                pokes.append(socket.create_connection(("127.0.0.1", ports["bolt"]), timeout=2))
            except OSError:
                break
            time.sleep(0.005)
        time.sleep(0.5)
        if fd_count(proc.pid) < FD_LIMIT - 1:
            pytest.skip(f"could not reach fd limit, at {fd_count(proc.pid)}/{FD_LIMIT}")

        # Connections to the other listeners complete in the kernel backlog; their accept() fails with EMFILE.
        for name in ("ws", "metrics", "rpc"):
            pokes.append(socket.create_connection(("127.0.0.1", ports[name]), timeout=2))

        if not retry(lambda: accept_log_lines(log_path) > 0, 3):
            pytest.skip(f"fd count {fd_count(proc.pid)}/{FD_LIMIT} but no accept errors logged; limit not hit")

        cpu_before = thread_cpu_seconds(proc.pid)
        log_size_before = os.path.getsize(log_path)
        time.sleep(HOLD_SECONDS)
        cpu_after = thread_cpu_seconds(proc.pid)
        log_growth = os.path.getsize(log_path) - log_size_before
        worst_name, worst_cpu = max(
            ((cpu_after[t][0], cpu_after[t][1] - cpu_before.get(t, (None, 0.0))[1]) for t in cpu_after),
            key=lambda x: x[1],
        )
        accept_lines = accept_log_lines(log_path)

        for s in pokes:
            s.close()
        pokes.clear()
        assert retry(lambda: fd_count(proc.pid) < FD_LIMIT - 10, 15), "fds not released"
        time.sleep(1.5)

        yield {
            "ports": ports,
            "have_metrics": have_metrics,
            "log_path": log_path,
            "worst_thread": (worst_name, worst_cpu),
            "log_growth": log_growth,
            "accept_lines": accept_lines,
        }
    finally:
        for s in pokes:
            s.close()
        proc.terminate()
        try:
            proc.wait(30)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_no_busy_loop_or_log_flood_at_fd_limit(scenario):
    name, cpu = scenario["worst_thread"]
    assert cpu < MAX_THREAD_CPU_SECONDS, f"thread '{name}' burned {cpu:.2f}s CPU in {HOLD_SECONDS}s while at fd limit"
    assert (
        scenario["accept_lines"] <= MAX_ACCEPT_LOG_LINES
    ), f"{scenario['accept_lines']} accept error log lines while at fd limit"
    assert scenario["log_growth"] < 1_000_000, f"log grew by {scenario['log_growth']} bytes while at fd limit"


def probe_bolt(scenario):
    return retry(lambda: bolt_works(scenario["ports"]["bolt"]))


def probe_websocket(scenario):
    return retry(lambda: ws_handshake_works(scenario["ports"]["ws"]))


def probe_metrics(scenario):
    if not scenario["have_metrics"]:
        pytest.skip("metrics server not available (enterprise license required)")
    return retry(lambda: metrics_ok(scenario["ports"]["metrics"]))


def probe_rpc(scenario):
    before = accepted_connection_lines(scenario["log_path"])
    with socket.create_connection(("127.0.0.1", scenario["ports"]["rpc"]), timeout=2):
        return retry(lambda: accepted_connection_lines(scenario["log_path"]) > before)


@pytest.mark.parametrize(
    "probe",
    [probe_bolt, probe_websocket, probe_metrics, probe_rpc],
    ids=["bolt", "websocket", "metrics_http", "rpc"],
)
def test_listener_accepts_after_release(scenario, probe):
    assert probe(scenario)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
