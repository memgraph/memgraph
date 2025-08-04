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


def validate_telemetry(item: dict) -> bool:
    """
    Simple check to see that we have the required fields.
    These are the bare minimum.
    """
    required_fields = [
        "data",
        "event",
        "machine_id",
        "run_id",
        "timestamp",
    ]

    for field in required_fields:
        if field not in item:
            return False

    return True


def get_ip_address(headers):
    """Extract IP address from headers similar to Lambda API"""
    if "X-Forwarded-For" in headers:
        return headers["X-Forwarded-For"].split(",")[0]
    return "0.0.0.0"


def build_handler(storage, args):
    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            assert False

        def do_GET(self):
            # Redirect to memgraph.com like the Lambda API
            self.send_response(302)
            self.send_header("Location", "https://memgraph.com")
            self.end_headers()
            return

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

            # Check if this is a memgraph user agent
            ua = self.headers.get("User-Agent", "")
            if not ua.startswith("memgraph"):
                self.send_response(302)
                self.send_header("Location", "https://memgraph.com")
                self.end_headers()
                return

            # Check content type
            content_type = self.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "msg": "invalid JSON"}).encode("utf-8"))
                return

            # Parse JSON body
            try:
                content_len = int(self.headers.get("content-length", 0))
                body_data = self.rfile.read(content_len).decode("utf-8")
                body = json.loads(body_data)
            except (json.JSONDecodeError, ValueError):
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "msg": "invalid JSON"}).encode("utf-8"))
                return

            # Normalize to list for uniform handling
            items = body if isinstance(body, list) else [body]

            ip = get_ip_address(self.headers)

            # Process each item
            for item in items:
                # Default to telemetry if no "type" field
                dtype = item.get("type", "telemetry")

                if dtype == "license-check":
                    # Skip license-check validation for this test
                    continue
                else:
                    item["type"] = "telemetry"
                    if not validate_telemetry(item):
                        self.send_response(400)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"success": False, "msg": "invalid telemetry"}).encode("utf-8"))
                        return

                # Annotate with IP (simulating Lambda behavior)
                item["ip"] = ip

                # Store for verification
                storage.append(item)

            if args.no_response_count > 0:
                args.no_response_count -= 1
                return

            if args.wrong_code_count > 0:
                args.wrong_code_count -= 1
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "msg": "server error"}).encode("utf-8"))
                return

            if args.hang:
                time.sleep(20)

            # Simulate S3 upload success
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"success": True}).encode("utf-8"))

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
    if not storage:
        print("No telemetry data received")
        return

    rid = storage[0]["run_id"]
    timestamp = 0

    print(f"Verifying {len(storage)} telemetry items...")

    for i, item in enumerate(storage):
        # Verify required fields are present
        assert "data" in item, f"Missing 'data' field in item {i}"
        assert "event" in item, f"Missing 'event' field in item {i}"
        assert "machine_id" in item, f"Missing 'machine_id' field in item {i}"
        assert "run_id" in item, f"Missing 'run_id' field in item {i}"
        assert "timestamp" in item, f"Missing 'timestamp' field in item {i}"
        assert "type" in item, f"Missing 'type' field in item {i}"
        assert "ip" in item, f"Missing 'ip' field in item {i}"

        # Verify type is telemetry
        assert item["type"] == "telemetry", f"Expected type 'telemetry', got '{item['type']}'"

        # Verify run_id consistency
        assert item["run_id"] == rid, f"Run ID mismatch: expected {rid}, got {item['run_id']}"

        # Verify timestamp ordering
        assert item["timestamp"] >= timestamp, f"Timestamp ordering violation: {item['timestamp']} < {timestamp}"
        timestamp = item["timestamp"]

        # Verify machine_id consistency
        assert (
            item["machine_id"] == storage[0]["machine_id"]
        ), f"Machine ID mismatch: expected {storage[0]['machine_id']}, got {item['machine_id']}"

        print(f"Item {i}: {item['event']} at {item['timestamp']} (IP: {item['ip']})")
        print(f"  Data keys: {list(item['data'].keys())}")
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--path", type=str, default="/88b5e7e8-746a-11e8-9f85-538a9e9690cc")
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
    startups = [[storage[0]]] if storage else []
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
    if storage:
        machine_ids = set(map(lambda x: x["machine_id"], itertools.chain(*startups)))
        assert len(machine_ids) == 1, f"Multiple machine IDs found: {machine_ids}"

    print("All telemetry validation passed!")
