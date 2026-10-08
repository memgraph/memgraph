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
SystemRestore opens its recovery stream after releasing the system lock, so a system tx committed in that window
reaches the replica first and the stale recovery must not wipe it. The race is hit by committing a parameter at a
random offset after the recovery heartbeat; seeding many parameters widens the snapshot phase.
"""

import asyncio
import os
import random
import shutil
import socket
import subprocess
import sys
import time

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.environ.get(
    "MG_E2E_BINARY", os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
)

BOLT_PORTS = {"main": 20000, "replica": 20001}
REPLICATION_PORT = 20002
PROXY_PORT = 20003
PROXY_CONTROL_PORT = 20004
file = "system_restore_race"

SEEDED_PARAMETERS = 20000
ROUNDS = 25
MAX_OFFSET_SEC = 0.04


class SeverableProxy:
    """TCP proxy in front of the replication port. Runs in its own process: mgclient holds the GIL during a query."""

    def __init__(self):
        self._proc = subprocess.Popen(
            [
                sys.executable,
                os.path.realpath(__file__),
                "--proxy",
                str(PROXY_PORT),
                str(REPLICATION_PORT),
                str(PROXY_CONTROL_PORT),
            ]
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                self._command("ping")
                return
            except OSError:
                time.sleep(0.05)
        self.stop()
        raise RuntimeError("proxy did not start")

    def _command(self, name):
        with socket.create_connection(("127.0.0.1", PROXY_CONTROL_PORT), timeout=10) as s:
            s.sendall(name.encode() + b"\n")
            return s.makefile().readline().strip()

    def heartbeat_time(self):
        """Time of the first MAIN->replica bytes since the last heal (the replica checker's heartbeat), or None."""
        reply = self._command("heartbeat")
        return None if reply == "none" else float(reply)

    def sever(self):
        self._command("sever")

    def heal(self):
        self._command("heal")

    def stop(self):
        self._proc.terminate()
        try:
            self._proc.wait(10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()


async def _proxy_main(listen_port, target_port, control_port):
    writers = set()
    state = {"down": False, "heartbeat": None}

    async def pipe(reader, writer, from_main):
        try:
            while data := await reader.read(65536):
                if from_main and state["heartbeat"] is None and not state["down"]:
                    state["heartbeat"] = time.time()
                writer.write(data)
                await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()

    async def handle(client_reader, client_writer):
        if state["down"]:
            client_writer.close()
            return
        try:
            up_reader, up_writer = await asyncio.open_connection("127.0.0.1", target_port)
        except OSError:
            client_writer.close()
            return
        writers.update((client_writer, up_writer))
        await asyncio.gather(pipe(client_reader, up_writer, True), pipe(up_reader, client_writer, False))
        writers.difference_update((client_writer, up_writer))

    async def control(reader, writer):
        command = (await reader.readline()).decode().strip()
        reply = "ok"
        if command == "sever":
            state["down"] = True
            for w in list(writers):
                w.close()
            writers.clear()
        elif command == "heal":
            state["down"] = False
            state["heartbeat"] = None
        elif command == "heartbeat":
            reply = "none" if state["heartbeat"] is None else repr(state["heartbeat"])
        writer.write(reply.encode() + b"\n")
        await writer.drain()
        writer.close()

    data_server = await asyncio.start_server(handle, "127.0.0.1", listen_port)
    control_server = await asyncio.start_server(control, "127.0.0.1", control_port)
    async with data_server, control_server:
        await asyncio.gather(data_server.serve_forever(), control_server.serve_forever())


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)
    time.sleep(1)


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture
def clean_dirs(test_name):
    for path in (
        os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name)),
        os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "logs", get_logs_path(file, test_name)),
    ):
        if os.path.exists(path):
            shutil.rmtree(path)
    yield


@pytest.fixture
def proxy():
    p = SeverableProxy()
    yield p
    p.stop()


def _instances(test_name):
    return {
        "replica": {
            "args": ["--bolt-port", str(BOLT_PORTS["replica"]), "--log-level=TRACE"],
            "log_file": f"{get_logs_path(file, test_name)}/replica.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica",
            "setup_queries": [f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORT};"],
        },
        "main": {
            "args": [
                "--bolt-port",
                str(BOLT_PORTS["main"]),
                "--log-level=TRACE",
                "--replication-replica-check-frequency-sec=1",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [],
        },
    }


def _wait_replica_ready(cursor):
    def system_status():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")[0][3]["status"]

    mg_sleep_and_assert("ready", system_status, time_between_attempt=0.05)


def _parameters(cursor):
    return sorted(execute_and_fetch_all(cursor, "SHOW PARAMETERS;"))


def test_system_restore_keeps_concurrent_system_tx(test_name, clean_dirs, proxy):
    interactive_mg_runner.start_all(_instances(test_name), keep_directories=False)
    main = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()
    poller = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()
    writer = connect(host="localhost", port=BOLT_PORTS["main"]).cursor()
    replica = connect(host="localhost", port=BOLT_PORTS["replica"]).cursor()

    # Seeded before the replica is registered so it costs no replication round trips.
    value = "x" * 256
    for n in range(SEEDED_PARAMETERS):
        execute_and_fetch_all(main, f'SET GLOBAL PARAMETER seed_{n}="{value}";')
    execute_and_fetch_all(main, f"REGISTER REPLICA replica SYNC TO '127.0.0.1:{PROXY_PORT}';")
    _wait_replica_ready(poller)

    for r in range(ROUNDS):
        proxy.sever()
        # The undeliverable delta makes MAIN mark the replica BEHIND.
        execute_and_fetch_all(writer, f'SET GLOBAL PARAMETER behind_{r}="x";')
        proxy.heal()

        heartbeat = None
        while heartbeat is None:
            heartbeat = proxy.heartbeat_time()
        time.sleep(max(0.0, heartbeat + random.uniform(0, MAX_OFFSET_SEC) - time.time()))
        execute_and_fetch_all(writer, f'SET GLOBAL PARAMETER raced_{r}="x";')

        _wait_replica_ready(poller)
        assert _parameters(replica) == _parameters(main), f"round {r}: replica is ready but its parameters differ"


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--proxy":
        asyncio.run(_proxy_main(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])))
        sys.exit(0)
    sys.exit(pytest.main([__file__, "-rA"]))
