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


class ServerHandler(SimpleHTTPRequestHandler):
    def do_POST(self):
        assert self.headers["user-agent"] == "memgraph/license-info", f"The header is {self.headers['user-agent']}"
        assert self.headers["accept"] == "application/json", f"The header is {self.headers['accept']}"
        assert self.headers["content-type"] == "application/json", f"The header is {self.headers['content-type']}"

        content_len = int(self.headers.get("content-length", 0))
        data = json.loads(self.rfile.read(content_len).decode("utf-8"))

        assert isinstance(data, dict)

        assert "run_id" in data
        assert "machine_id" in data
        assert "type" in data
        assert "license_type" in data
        assert "license_key" in data
        assert "organization" in data
        assert "memory" in data
        assert "memory_limit" in data
        assert "valid" in data
        assert "memory_limit" in data
        assert "timestamp" in data

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
