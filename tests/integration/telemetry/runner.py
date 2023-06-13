#!/usr/bin/python3 -u

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))


def execute_test(**kwargs):
    client_binary = kwargs.pop("client")
    server_binary = kwargs.pop("server")
    storage_directory = kwargs.pop("storage")

    start_server = kwargs.pop("start_server", True)
    endpoint = kwargs.pop("endpoint", "")
    interval = kwargs.pop("interval", 1)
    duration = kwargs.pop("duration", 5)

    timeout = duration * 2 if "hang" not in kwargs else duration * 2 + 60
    success = False

    server_args = [server_binary, "--interval", interval, "--duration", duration]
    for flag, value in kwargs.items():
        flag = "--" + flag.replace("_", "-")
        # We handle boolean flags here. The type of value must be `bool`, and
        # the value must be `True` to supply a boolean flag (a flag that only
        # has --flag, without the value).
        if value is True:
            server_args.append(flag)
        else:
            server_args.extend([flag, value])

    client_args = [
        client_binary,
        "--interval",
        interval,
        "--duration",
        duration,
        "--storage-directory",
        storage_directory,
    ]
    if endpoint:
        client_args.extend(["--endpoint", endpoint])

    server = None
    if start_server:
        server = subprocess.Popen(list(map(str, server_args)))
        time.sleep(0.4)
        assert server.poll() is None, "Server process died prematurely!"

    try:
        subprocess.run(list(map(str, client_args)), timeout=timeout, check=True)
    finally:
        if server is None:
            success = True
        else:
            server.terminate()
            try:
                success = server.wait(timeout=5) == 0
            except subprocess.TimeoutExpired:
                server.kill()
    return success


TESTS = [
    {},
    {"interval": 2},
    {"duration": 10},
    {"interval": 2, "duration": 10},
    {"redirect": True},
    {"no_response_count": 2},
    {"wrong_code_count": 2},
    {"hang": True, "duration": 0},
    {"path": "/nonexistent/", "no_check": True},
    {"endpoint": "http://127.0.0.1:9000/nonexistent/", "no_check": True},
    {"start_server": False},
    {"startups": 4, "no_check_duration": True},  # the last 3 tests failed
    # to send any data + this test
    {"add_garbage": True},
]

if __name__ == "__main__":
    server_binary = os.path.join(SCRIPT_DIR, "server.py")
    client_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "telemetry", "client")
    kvstore_console_binary = os.path.join(PROJECT_DIR, "build", "tests", "manual", "kvstore_console")

    parser = argparse.ArgumentParser()
    parser.add_argument("--client", default=client_binary)
    parser.add_argument("--server", default=server_binary)
    parser.add_argument("--kvstore-console", default=kvstore_console_binary)
    args = parser.parse_args()

    storage = tempfile.TemporaryDirectory()

    for test in TESTS:
        print("\033[1;36m~~ Executing test with arguments:", json.dumps(test, sort_keys=True), "~~\033[0m")

        if test.pop("add_garbage", False):
            proc = subprocess.Popen(
                [args.kvstore_console, "--path", storage.name], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL
            )
            proc.communicate("put garbage garbage".encode("utf-8"))
            assert proc.wait() == 0

        try:
            success = execute_test(client=args.client, server=args.server, storage=storage.name, **test)
        except Exception as e:
            print("\033[1;33m", e, "\033[0m", sep="")
            success = False

        if not success:
            print("\033[1;31m~~", "Test failed!", "~~\033[0m")
            sys.exit(1)
        else:
            print("\033[1;32m~~", "Test ok!", "~~\033[0m")

    sys.exit(0)
