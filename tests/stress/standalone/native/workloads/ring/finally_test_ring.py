#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
Phase 3 of the ring stress test (Memgraph port of antithesishq/dgraph
ring_test/finally_test_ring.py, without the Antithesis SDK).

For each node name, walk :FRIEND edges and assert that:
  * after exactly N hops the walk returns to its starting node;
  * exactly N distinct nodes are visited (no chord, no premature cycle).
"""

import os
import sys
import time

sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")),
)

from common import argument_session, connection_argument_parser


def parse_args():
    parser = connection_argument_parser()
    parser.add_argument("--node-count-file", default="/tmp/ring_node_count")
    parser.add_argument(
        "--wait-seconds", type=int, default=0, help="Sleep before verification (e.g. wait for replicas)."
    )
    return parser.parse_args()


def walk_ring(session, start_name, num_nodes):
    """Walk N hops from `start_name` and verify we close the ring."""
    result = session.run(
        "MATCH (n:Node {name: $name}) RETURN id(n) AS id",
        name=start_name,
    )
    rows = [r["id"] for r in result]
    if not rows:
        return False, f"start node '{start_name}' not found"
    start_id = rows[0]

    visited = set()
    current = start_id
    for _ in range(num_nodes - 1):
        if current in visited:
            return False, f"visited twice at {current}"
        visited.add(current)
        nxt = session.run(
            "MATCH (a:Node)-[:FRIEND]->(b:Node) WHERE id(a) = $id RETURN id(b) AS id",
            id=current,
        ).data()
        if not nxt:
            return False, f"terminal node at {current}"
        if len(nxt) > 1:
            return False, f"node {current} has out-degree {len(nxt)} (>1)"
        current = nxt[0]["id"]

    nxt = session.run(
        "MATCH (a:Node)-[:FRIEND]->(b:Node) WHERE id(a) = $id RETURN id(b) AS id",
        id=current,
    ).data()
    if not nxt:
        return False, f"terminal node at {current} (last hop)"
    if nxt[0]["id"] != start_id:
        return False, f"last hop went to {nxt[0]['id']}, expected start {start_id}"
    if len(visited) != num_nodes - 1:
        return False, f"visited {len(visited)} distinct nodes, expected {num_nodes - 1}"
    return True, "ok"


def check_ring(args):
    with open(args.node_count_file) as f:
        num_nodes = int(f.read())

    with argument_session(args) as session:
        for i in range(num_nodes):
            ok, msg = walk_ring(session, str(i), num_nodes)
            assert ok, f"Ring continuity broken starting at {i}: {msg}"

    print(f"Ring of {num_nodes} nodes is intact.")


if __name__ == "__main__":
    args = parse_args()
    if args.wait_seconds:
        time.sleep(args.wait_seconds)
    check_ring(args)
