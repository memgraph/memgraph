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
import time

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))


def execute_test(**kwargs):
    client_binary = kwargs.pop("client")
    server_binary = kwargs.pop("server")

    start_server = kwargs.pop("start_server", True)
    interval = kwargs.pop("interval", 1)
    duration = kwargs.pop("duration", 5)
    license_type = kwargs.pop("license-type", "enterprise")

    timeout = duration * 2 if "hang" not in kwargs else duration * 2 + 60
    success = False

    client_args = [client_binary, "--interval", interval, "--duration", duration, "--license-type", license_type]

    server = None
    if start_server:
        server = subprocess.Popen(server_binary)
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
                success = True
            except subprocess.TimeoutExpired:
                server.kill()
    return success


def main():
    server_binary = os.path.join(SCRIPT_DIR, "server.py")
    client_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "license_info", "client")

    parser = argparse.ArgumentParser()
    parser.add_argument("--client", default=client_binary)
    parser.add_argument("--server", default=server_binary)
    parser.add_argument("--server-url", default="127.0.0.1")
    parser.add_argument("--server-port", default="5500")
    args = parser.parse_args()

    tests = [
        {"interval": 2},
        {"duration": 10},
        {"interval": 2, "duration": 10},
        {"license-type": "oem"},
        {"license-type": "enterprise"},
    ]
    for test in tests:
        print("\033[1;36m~~ Executing test with arguments:", json.dumps(test, sort_keys=True), "~~\033[0m")

        try:
            success = execute_test(client=args.client, server=args.server, **test)
        except Exception as e:
            print("\033[1;33m", e, "\033[0m", sep="")
            success = False

        if not success:
            print("\033[1;31m~~", "Test failed!", "~~\033[0m")
            sys.exit(1)
        else:
            print("\033[1;32m~~", "Test ok!", "~~\033[0m")
    sys.exit(0)


if __name__ == "__main__":
    main()
