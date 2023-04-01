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
from http.server import HTTPServer, SimpleHTTPRequestHandler

EXPECTED_LICENSE_INFO_FIELDS = {
    "run_id": str,
    "machine_id": str,
    "type": str,
    "license_type": str,
    "license_key": str,
    "organization": str,
    "valid": bool,
    "physical_memory_size": int,
    "swap_memory_size": int,
    "memory_used": int,
    "runtime_memory_limit": int,
    "license_memory_limit": int,
    "timestamp": float,
}


class ServerHandler(SimpleHTTPRequestHandler):
    def do_POST(self):
        assert self.headers["user-agent"] == "memgraph/license-info", f"The header is {self.headers['user-agent']}"
        assert self.headers["accept"] == "application/json", f"The header is {self.headers['accept']}"
        assert self.headers["content-type"] == "application/json", f"The header is {self.headers['content-type']}"

        content_len = int(self.headers.get("content-length", 0))
        data = json.loads(self.rfile.read(content_len).decode("utf-8"))

        assert isinstance(data, dict)

        for expected_field, expected_type in EXPECTED_LICENSE_INFO_FIELDS.items():
            assert expected_field in data, f"Field {expected_field} not found in received data"
            assert isinstance(
                data[expected_field], expected_type
            ), f"Field {expected_field} is not correct type: expected {expected_type} got {type(data[expected_field])}"
        assert len(EXPECTED_LICENSE_INFO_FIELDS) == len(data), "Expected data size does not match received"

        self.send_response(200)
        self.end_headers()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5500)
    args = parser.parse_args()

    with HTTPServer((args.address, args.port), ServerHandler) as srv:
        print(f"Serving HTTP server at {args.address}:{args.port}")
        srv.serve_forever()


if __name__ == "__main__":
    main()
