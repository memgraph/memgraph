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
import itertools
import json
import os
import signal
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


def build_handler(storage, args):
    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            assert False

        def do_GET(self):
            assert False

        def do_PUT(self):
            assert False

        def do_POST(self):
            if args.redirect and self.path == args.path:
                # 307 is used instead of 301 to preserve body data
                # https://stackoverflow.com/questions/19070801/curl-loses-body-when-a-post-redirected-from-http-to-https
                self.send_response(307)
                self.send_header("Location", args.redirect_path)
                self.end_headers()
                return

            assert self.headers["user-agent"] == "memgraph/telemetry"
            assert self.headers["accept"] == "application/json"
            assert self.headers["content-type"] == "application/json"

            content_len = int(self.headers.get("content-length", 0))
            data = json.loads(self.rfile.read(content_len).decode("utf-8"))

            if self.path not in [args.path, args.redirect_path]:
                self.send_response(404)
                self.end_headers()
                return

            if args.no_response_count > 0:
                args.no_response_count -= 1
                return

            if args.wrong_code_count > 0:
                args.wrong_code_count -= 1
                self.send_response(500)
                self.end_headers()
                return

            assert type(data) == list

            for item in data:
                assert type(item) == dict
                assert "event" in item
                assert "run_id" in item
                assert "type" in item
                assert "machine_id" in item
                assert "data" in item
                assert "timestamp" in item
                storage.append(item)

            if args.hang:
                time.sleep(20)

            self.send_response(200)
            self.end_headers()

    return Handler


class Server(HTTPServer):
    def handle_error(self, request, client_address):
        super().handle_error(request, client_address)
        os._exit(1)

    def shutdown(self):
        # TODO: this is a hack. The parent object implementation of this
        # function sets the shutdown flag and then waits for the shutdown to
        # complete.  We only need to set the shutdown flag because we don't
        # want to run the server in another thread. The parent implementation
        # can be seen here:
        # https://github.com/python/cpython/blob/3.5/Lib/socketserver.py#L241
        self._BaseServer__shutdown_request = True


def item_sort_key(obj):
    if type(obj) != dict:
        return -1
    if "timestamp" not in obj:
        return -1
    return obj["timestamp"]


def verify_storage(storage, args):
    rid = storage[0]["run_id"]
    version = storage[0]["version"]
    assert version != ""
    timestamp = 0
    for i, item in enumerate(storage):
        assert item["run_id"] == rid
        assert item["version"] == version
        print(item)
        print("\n")

        assert item["timestamp"] >= timestamp
        timestamp = item["timestamp"]

        if i == 0:
            assert item["event"] == "startup"
        elif i == len(storage) - 1:
            assert item["event"] == "shutdown"
        else:
            assert item["event"] == i - 1

        if i == 0:
            assert "architecture" in item["data"]
            assert "cpu_count" in item["data"]
            assert "cpu_model" in item["data"]
            assert "kernel" in item["data"]
            assert "memory" in item["data"]
            assert "os" in item["data"]
            assert "swap" in item["data"]
        else:
            assert "ssl" in item

            # User defined data
            assert item["data"]["test"]["vertices"] == i
            assert item["data"]["test"]["edges"] == i

            # Global data
            assert "resources" in item["data"]
            assert "cpu" in item["data"]["resources"]
            assert "memory" in item["data"]["resources"]
            assert "disk" in item["data"]["resources"]
            assert "uptime" in item["data"]

            uptime = item["data"]["uptime"]
            expected = i * args.interval
            if i == len(storage) - 1:
                if not args.no_check_duration:
                    expected = args.duration
                else:
                    expected = uptime
            assert uptime >= expected - 4 and uptime <= expected + 4

            # Memgraph specific data
            # TODO Missing clients and other usage based data
            assert "client" in item["data"]
            assert "database" in item["data"]
            assert "disk_usage" in item["data"]["database"][0]
            assert "durability" in item["data"]["database"][0]
            assert "WAL_enabled" in item["data"]["database"][0]["durability"]
            assert "snapshot_enabled" in item["data"]["database"][0]["durability"]
            assert "edges" in item["data"]["database"][0]
            assert "existence_constraints" in item["data"]["database"][0]
            assert "isolation_level" in item["data"]["database"][0]
            assert "label_indices" in item["data"]["database"][0]
            assert "label_prop_indices" in item["data"]["database"][0]
            assert "ram_usage" in item["data"]["database"][0]
            assert "storage_mode" in item["data"]["database"][0]
            assert "unique_constraints" in item["data"]["database"][0]
            assert "vertices" in item["data"]["database"][0]
            assert "event_counters" in item["data"]
            assert "exception" in item["data"]
            assert "query" in item["data"]
            assert "first_failed_query" in item["data"]["query"]
            assert "first_successful_query" in item["data"]["query"]
            assert "query_module_counters" in item["data"]
            assert "replication" in item["data"]
            assert "async" in item["data"]["replication"]
            assert "sync" in item["data"]["replication"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--path", type=str, default="/")
    parser.add_argument("--redirect", action="store_true")
    parser.add_argument("--no-response-count", type=int, default=0)
    parser.add_argument("--wrong-code-count", type=int, default=0)
    parser.add_argument("--no-check", action="store_true")
    parser.add_argument("--hang", action="store_true")
    parser.add_argument("--interval", type=int, default=1)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--startups", type=int, default=1)
    parser.add_argument("--no-check-duration", action="store_true")
    args = parser.parse_args()
    args.redirect_path = os.path.join(args.path, "redirect")

    storage = []
    handler = build_handler(storage, args)
    httpd = Server((args.address, args.port), handler)
    signal.signal(signal.SIGTERM, lambda signum, frame: httpd.shutdown())
    httpd.serve_forever()
    httpd.server_close()

    if args.no_check:
        sys.exit(0)

    # Order the received data.
    storage.sort(key=item_sort_key)

    # Split the data into individual startups.
    startups = [[storage[0]]]
    for item in storage[1:]:
        if item["run_id"] != startups[-1][-1]["run_id"]:
            startups.append([])
        startups[-1].append(item)

    # Check that there were the correct number of startups.
    assert len(startups) == args.startups, f"Expected: {args.startups}, actual: {len(startups)}"

    # Verify each startup.
    for startup in startups:
        verify_storage(startup, args)

    # machine id has to be same for every run on the same machine
    assert len(set(map(lambda x: x["machine_id"], itertools.chain(*startups)))) == 1
