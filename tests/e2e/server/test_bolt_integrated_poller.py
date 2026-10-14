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
Plain-TCP Bolt sessions must run in the integrated-poller ("raw") mode under the default scheduler.

Server shutdown logs `Bolt poller stats: adopted=.. inline_claims=.. monitor_claims=..`; asserting on it makes a
run that silently fell back to asio (poller disabled, non-default scheduler, TLS) fail instead of pass.
"""

import base64
import multiprocessing
import os
import re
import secrets
import signal
import socket
import struct
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import interactive_mg_runner
import mgclient
import pytest

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

NUM_WORKERS = 4
SHARED_PORT = 30711
COUNT_PORT = 30712
SHUTDOWN_PORT = 30713
DISABLED_PORT = 30714

SERVER_ARGS = ["--bolt-num-workers", str(NUM_WORKERS), "--log-level=INFO"]

BOLT_PREAMBLE = b"\x60\x60\xb0\x17"
BOLT_5_2 = b"\x00\x00\x02\x05"
BOLT_HANDSHAKE = BOLT_PREAMBLE + BOLT_5_2 + b"\x00" * 12

# ActiveTCPSessions seen by a connection that is the only one open (the gauge counts the observer itself).
MONITOR_ONLY = 1

STATS_RE = re.compile(r"Bolt poller stats: adopted=(\d+) inline_claims=(\d+) monitor_claims=(\d+)")


@dataclass(frozen=True, slots=True)
class PollerStats:
    adopted: int
    inline_claims: int
    monitor_claims: int

    @property
    def claims(self) -> int:
        return self.inline_claims + self.monitor_claims


def _log_files(name: str) -> list[Path]:
    # Memgraph's --log-file is a daily rotating sink: `<name>.log` becomes `<name>_<YYYY-MM-DD>.log`.
    return sorted(Path(interactive_mg_runner.BUILD_DIR, "e2e", "logs", "server").glob(f"{name}_*.log"))


def start_instance(name: str, port: int, extra_args: list[str] | None = None) -> None:
    log_file = f"server/{name}.log"
    for stale in _log_files(name):
        stale.unlink()
    context = {
        name: {
            "args": ["--bolt-port", str(port)] + SERVER_ARGS + (extra_args or []),
            "log_file": log_file,
            "data_directory": f"server/{name}",
            "setup_queries": [],
        }
    }
    interactive_mg_runner.start_all_keep_others(context)


def stop_only(name: str):
    instance = interactive_mg_runner.MEMGRAPH_INSTANCES[name]
    instance.stop(keep_directories=False)
    interactive_mg_runner.MEMGRAPH_INSTANCES.pop(name)
    return instance


def stop_instance(name: str) -> PollerStats:
    """Stats are logged only on graceful shutdown (Server::AwaitShutdown), so they are read after a clean stop."""
    stop_only(name)
    return read_stats(name)


def read_log(name: str) -> str:
    return "".join(log.read_text(encoding="utf-8", errors="replace") for log in _log_files(name))


def read_stats(name: str) -> PollerStats:
    text = read_log(name)
    assert "Bolt using the integrated poller" in text, "integrated poller was not enabled at startup"
    match = STATS_RE.search(text)
    assert match is not None, "no 'Bolt poller stats' line in the shutdown log"
    return PollerStats(*(int(group) for group in match.groups()))


def connect(port: int) -> mgclient.Connection:
    connection = mgclient.connect(host="127.0.0.1", port=port)
    connection.autocommit = True
    return connection


def fetch_all(cursor, query: str, params: dict | None = None) -> list[tuple]:
    cursor.execute(query, params or {})
    return cursor.fetchall()


def assert_healthy(port: int) -> None:
    connection = connect(port)
    try:
        assert fetch_all(connection.cursor(), "RETURN 42") == [(42,)]
    finally:
        connection.close()


def active_tcp_sessions(cursor) -> int:
    rows = fetch_all(cursor, "SHOW METRICS INFO")
    return next(row[3] for row in rows if row[0] == "ActiveTCPSessions")


def wait_for_session_count(cursor, expected: int, timeout_s: float = 10.0) -> int:
    """
    Teardown is asynchronous, so poll until the gauge is exactly `expected` (it counts the polling cursor's session).
    Exact, not `<=`, so a leaked session cannot be masked by an unrelated one finishing its teardown.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        count = active_tcp_sessions(cursor)
        if count == expected or time.monotonic() >= deadline:
            return count
        time.sleep(0.05)


def raw_socket(port: int) -> socket.socket:
    return socket.create_connection(("127.0.0.1", interactive_mg_runner.effective_port(port)), timeout=5)


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            break
        data += chunk
    return data


def assert_server_closes(sock: socket.socket) -> None:
    """Garbage must make the server close the peer, not wait for more bytes."""
    sock.settimeout(5)
    try:
        while sock.recv(4096):
            pass
    except ConnectionResetError:
        pass
    except socket.timeout:
        pytest.fail("server did not close a connection that sent garbage")


@pytest.fixture(scope="module")
def shared_port():
    start_instance("poller_shared", SHARED_PORT)
    yield SHARED_PORT
    # Adoption proof for every test on the shared instance (~1000 queries on one connection => >= 1000 claims).
    stats = stop_instance("poller_shared")
    assert stats.adopted >= NUM_WORKERS, f"sessions were not adopted by the poller: {stats}"
    assert stats.claims >= 1000, f"the poller did not claim the ready sessions: {stats}"


def test_many_queries_on_one_connection(shared_port):
    connection = connect(shared_port)
    cursor = connection.cursor()
    for i in range(1000):
        assert fetch_all(cursor, "RETURN $i * 2", {"i": i}) == [(i * 2,)]
    connection.autocommit = False
    for i in range(50):
        assert fetch_all(cursor, "RETURN $i", {"i": i}) == [(i,)]
    connection.commit()
    connection.close()


def test_large_messages(shared_port):
    connection = connect(shared_port)
    cursor = connection.cursor()
    payload = "x" * (8 * 1024 * 1024)
    assert fetch_all(cursor, "RETURN size($s), $s", {"s": payload}) == [(len(payload), payload)]
    rows = fetch_all(cursor, "UNWIND range(1, 200000) AS i RETURN i")
    assert len(rows) == 200000
    assert rows[0] == (1,)
    assert rows[-1] == (200000,)
    assert sum(row[0] for row in rows) == 200000 * 200001 // 2
    assert fetch_all(cursor, "RETURN 1") == [(1,)]
    connection.close()


# (bytes sent, whether the handshake is completed first, whether the server must close the peer itself)
MALFORMED_PEERS = {
    **{f"drop_mid_handshake_{n}": (BOLT_HANDSHAKE[:n], False, False) for n in (1, 2, 4, 10, 19)},
    "garbage_before_handshake": (bytes(range(256)) * 16, False, True),
    # A well-framed chunk (4 bytes + end marker) whose payload is not a Bolt message.
    "bad_message_after_handshake": (struct.pack(">H", 4) + b"\xff\xff\xff\xff" + b"\x00\x00", True, True),
    # A chunk header announcing 1000 bytes of which only 10 arrive, then the peer vanishes.
    "drop_mid_chunk_after_handshake": (struct.pack(">H", 1000) + b"\xb1\x01" + b"\x00" * 8, True, False),
}


@pytest.mark.parametrize(
    "payload, handshake_first, server_closes", MALFORMED_PEERS.values(), ids=MALFORMED_PEERS.keys()
)
def test_malformed_peer(shared_port, payload, handshake_first, server_closes):
    monitor = connect(shared_port)
    monitor_cursor = monitor.cursor()
    settled = wait_for_session_count(monitor_cursor, MONITOR_ONLY)
    assert settled == MONITOR_ONLY, f"sessions of earlier tests were not torn down: ActiveTCPSessions {settled}"
    sock = raw_socket(shared_port)
    if handshake_first:
        sock.sendall(BOLT_HANDSHAKE)
        assert recv_exact(sock, 4) == BOLT_5_2
    if payload:
        sock.sendall(payload)
    if server_closes:
        assert_server_closes(sock)
    sock.close()
    assert_healthy(shared_port)
    remaining = wait_for_session_count(monitor_cursor, MONITOR_ONLY)
    monitor.close()
    assert remaining == MONITOR_ONLY, f"session leaked: ActiveTCPSessions {remaining} != {MONITOR_ONLY}"


def terminate_idle_session(port: int) -> None:
    victim = connect(port)
    victim_cursor = victim.cursor()
    for _ in range(5):
        fetch_all(victim_cursor, "RETURN 1")
    victim_uuid = fetch_all(victim_cursor, "SET SESSION TRACE OFF")[0][0]

    admin = connect(port)
    admin_cursor = admin.cursor()
    both = wait_for_session_count(admin_cursor, 2)
    assert both == 2, f"expected victim + admin sessions, got ActiveTCPSessions {both}"
    # An idle session has no query in flight, so TERMINATE must close it right away; the gauge dropping with
    # no victim traffic proves it was not left for a lazy close at the next request.
    assert fetch_all(admin_cursor, f"TERMINATE SESSIONS '{victim_uuid}'") == [(victim_uuid, True)]
    after = wait_for_session_count(admin_cursor, MONITOR_ONLY)
    assert after == MONITOR_ONLY, f"idle session was not closed by TERMINATE SESSIONS: ActiveTCPSessions {after}"

    with pytest.raises(mgclient.Error):
        fetch_all(victim_cursor, "RETURN 1")

    assert fetch_all(admin_cursor, "RETURN 1") == [(1,)]
    admin.close()
    victim.close()


def test_terminate_idle_raw_session(shared_port):
    terminate_idle_session(shared_port)


# 60M streamed rows; nested UNWINDs keep memory flat, unlike one huge range().
LONG_QUERY_OUTER = 12_000
LONG_QUERY_INNER = 5_000
LONG_QUERY_MARKER = "poller_long_query_marker"
LONG_QUERY = (
    f"UNWIND range(1, {LONG_QUERY_OUTER}) AS i UNWIND range(1, {LONG_QUERY_INNER}) AS j "
    f"WITH i, j WHERE (i + j) % 7 = 3 RETURN count(*) AS {LONG_QUERY_MARKER}"
)
LONG_QUERY_RESULT = sum(
    (LONG_QUERY_INNER - r) // 7 + 1 if r <= LONG_QUERY_INNER else 0
    for i in range(1, LONG_QUERY_OUTER + 1)
    for r in [(3 - i) % 7 or 7]
)


def wait_until_running(admin_cursor, victim: multiprocessing.Process, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(LONG_QUERY_MARKER in str(row) for row in fetch_all(admin_cursor, "SHOW TRANSACTIONS")):
            return
        assert victim.is_alive(), "the long query ended before it was observed running; increase LONG_QUERY_OUTER"
        time.sleep(0.01)
    pytest.fail(f"the long query was not observed running within {timeout} s")


def pack_str(text: str) -> bytes:
    data = text.encode()
    if len(data) < 16:
        return bytes([0x80 + len(data)]) + data
    return b"\xd0" + bytes([len(data)]) + data if len(data) < 256 else b"\xd1" + struct.pack(">H", len(data)) + data


def pack_map(entries: dict[str, bytes]) -> bytes:
    return bytes([0xA0 + len(entries)]) + b"".join(pack_str(key) + value for key, value in entries.items())


def bolt_message(tag: int, *fields: bytes) -> bytes:
    body = bytes([0xB0 + len(fields), tag]) + b"".join(fields)
    return struct.pack(">H", len(body)) + body + b"\x00\x00"


def read_bolt_message(sock: socket.socket) -> tuple[int, bytes]:
    """Return (message tag, whole payload) of the next Bolt message; the payload keeps the PackStream bytes."""
    payload = b""
    while True:
        header = recv_exact(sock, 2)
        if len(header) != 2:
            raise ConnectionError("the server closed the connection")
        (size,) = struct.unpack(">H", header)
        if size == 0:
            return payload[1], payload
        chunk = recv_exact(sock, size)
        if len(chunk) != size:
            raise ConnectionError("the server closed the connection")
        payload += chunk


BOLT_SUCCESS, BOLT_RECORD, BOLT_FAILURE = 0x70, 0x71, 0x7F
BOLT_HELLO, BOLT_LOGON, BOLT_RUN, BOLT_PULL = 0x01, 0x6A, 0x10, 0x3F
UUID_RE = re.compile(rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def bolt_run_pull(query: str) -> bytes:
    return bolt_message(BOLT_RUN, pack_str(query), pack_map({}), pack_map({})) + bolt_message(
        BOLT_PULL, pack_map({"n": b"\xff"})
    )


def run_long_query_victim(port: int, pipe) -> None:
    """
    Raw Bolt in its own process (it blocks on the socket): mgclient answers FAILURE with a RESET and then reports the
    send error on the closed socket, so an abort could not be told apart from a close of an idle session.
    """
    sock = raw_socket(port)
    sock.sendall(BOLT_HANDSHAKE)
    assert recv_exact(sock, 4) == BOLT_5_2
    sock.sendall(bolt_message(BOLT_HELLO, pack_map({"user_agent": pack_str("poller-test/1.0")})))
    assert read_bolt_message(sock)[0] == BOLT_SUCCESS
    sock.sendall(bolt_message(BOLT_LOGON, pack_map({"scheme": pack_str("none")})))
    assert read_bolt_message(sock)[0] == BOLT_SUCCESS

    sock.sendall(bolt_run_pull("SET SESSION TRACE OFF"))
    uuid = None
    for _ in range(3):  # RUN SUCCESS, RECORD, PULL SUCCESS
        tag, payload = read_bolt_message(sock)
        if tag == BOLT_RECORD:
            uuid = UUID_RE.search(payload).group().decode()
    pipe.send(("uuid", uuid))

    sock.sendall(bolt_run_pull(LONG_QUERY))
    try:
        # RUN SUCCESS, then either RECORD + PULL SUCCESS (completed) or FAILURE (aborted).
        outcome = ("completed", b"")
        for _ in range(3):
            tag, payload = read_bolt_message(sock)
            if tag == BOLT_FAILURE:
                outcome = ("failure", payload)
                break
        pipe.send(outcome)
    except (ConnectionError, socket.timeout) as error:
        pipe.send(("transport_error", str(error).encode()))

    # A terminated session is closed by the owner; an open one would block here until the timeout.
    sock.settimeout(5)
    try:
        closed = sock.recv(4096) == b""
    except ConnectionResetError:
        closed = True
    except socket.timeout:
        closed = False
    pipe.send(("closed", closed))


def recv_from_victim(pipe, timeout: float = 120.0) -> tuple[str, object]:
    assert pipe.poll(timeout), f"no message from the victim within {timeout} s (the in-flight query never returned?)"
    return pipe.recv()


def test_terminate_running_raw_session(shared_port):
    admin = connect(shared_port)
    admin_cursor = admin.cursor()

    parent_pipe, child_pipe = multiprocessing.Pipe()
    victim = multiprocessing.Process(target=run_long_query_victim, args=(shared_port, child_pipe))
    victim.start()
    try:
        kind, victim_uuid = recv_from_victim(parent_pipe)
        assert kind == "uuid", f"unexpected victim message {kind!r}: {victim_uuid!r}"
        both = wait_for_session_count(admin_cursor, 2)
        assert both == 2, f"expected victim + admin sessions, got ActiveTCPSessions {both}"

        wait_until_running(admin_cursor, victim)
        # Execute_ consumes the buffered PULL right after RUN; the sleep is only margin against hitting an idle session.
        time.sleep(0.1)
        assert fetch_all(admin_cursor, f"TERMINATE SESSIONS '{victim_uuid}'") == [(victim_uuid, True)]

        # A running session is closed by its owner at re-arm; a completed query or transport error means it was not
        # terminated while running.
        kind, value = recv_from_victim(parent_pipe)
        assert kind == "failure", f"the in-flight query was not aborted by TERMINATE SESSIONS: {kind!r}: {value!r}"
        assert b"asked to abort by another user" in value, f"unexpected abort message {value!r}"
        assert recv_from_victim(parent_pipe) == ("closed", True), "the session stayed open after TERMINATE SESSIONS"
    finally:
        victim.join(timeout=10)
        if victim.is_alive():
            victim.terminate()
            victim.join(timeout=5)

    after = wait_for_session_count(admin_cursor, MONITOR_ONLY)
    assert after == MONITOR_ONLY, f"running session was not closed after TERMINATE SESSIONS: ActiveTCPSessions {after}"
    assert fetch_all(admin_cursor, "RETURN 1") == [(1,)]
    admin.close()


def test_poller_disabled():
    start_instance("poller_disabled", DISABLED_PORT, ["--bolt-integrated-poller=false"])
    try:
        connection = connect(DISABLED_PORT)
        cursor = connection.cursor()
        for i in range(20):
            assert fetch_all(cursor, "RETURN $i", {"i": i}) == [(i,)]
        rows = fetch_all(cursor, "UNWIND range(1, 200000) AS i RETURN i")
        assert len(rows) == 200000
        assert rows[-1] == (200000,)
        connection.close()
        terminate_idle_session(DISABLED_PORT)
    finally:
        instance = stop_only("poller_disabled")
    assert instance.proc_mg.returncode == 0, f"memgraph exited with {instance.proc_mg.returncode} on SIGTERM"
    text = read_log("poller_disabled")
    assert "server is fully armed and operational" in text, "the server log is missing the startup line"
    assert "Bolt using the integrated poller" not in text, "a poller was created although it is disabled"
    assert STATS_RE.search(text) is None, "poller stats were logged although no poller exists"


def test_websocket_on_bolt_port(shared_port):
    sock = raw_socket(shared_port)
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    sock.sendall(
        (
            "GET / HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{shared_port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode()
    )
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        assert chunk, "connection closed during websocket upgrade"
        response += chunk
    assert response.startswith(b"HTTP/1.1 101"), response

    # Client frames must be masked: one FIN binary frame (0x82) carrying the Bolt handshake.
    mask = secrets.token_bytes(4)
    masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(BOLT_HANDSHAKE))
    sock.sendall(bytes([0x82, 0x80 | len(masked)]) + mask + masked)

    header = recv_exact(sock, 2)
    assert header[0] & 0x0F == 0x2, f"expected a binary frame, got {header!r}"
    assert header[1] == 4, f"expected a 4 byte payload, got {header!r}"
    assert recv_exact(sock, 4) == BOLT_5_2
    sock.close()
    assert_healthy(shared_port)


def _load_worker(port: int) -> None:
    try:
        cursor = connect(port).cursor()
        while True:
            cursor.execute("UNWIND range(1, 100) AS i RETURN i")
            cursor.fetchall()
    except mgclient.Error:
        pass


def test_graceful_shutdown_under_load():
    start_instance("poller_shutdown", SHUTDOWN_PORT)
    workers = [multiprocessing.Process(target=_load_worker, args=(SHUTDOWN_PORT,)) for _ in range(2 * NUM_WORKERS)]
    instance = interactive_mg_runner.MEMGRAPH_INSTANCES["poller_shutdown"]
    try:
        for worker in workers:
            worker.start()
        time.sleep(1)  # clients must be mid-query when SIGTERM arrives
        instance.proc_mg.send_signal(signal.SIGTERM)
        try:
            instance.proc_mg.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pytest.fail("memgraph did not shut down within 5 s under load")
    finally:
        for worker in workers:
            worker.terminate()
            worker.join(timeout=5)
        exit_code = instance.proc_mg.returncode
        stats = stop_instance("poller_shutdown")
    assert exit_code == 0, f"memgraph exited with {exit_code} on SIGTERM"
    assert stats.adopted >= NUM_WORKERS, stats


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
