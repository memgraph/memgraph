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
Phase 1 of the ring stress test (Memgraph port of antithesishq/dgraph
ring_test/first_make_ring.py, without the Antithesis SDK).

Builds a directed cycle of N nodes (:Node) connected by :FRIEND edges:
    0 -> 1 -> 2 -> ... -> N-1 -> 0

Each node has a `name` (string id). The chosen N is written to
--node-count-file so the modify and verify phases pick up the same size.
"""

import os
import random
import sys

sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")),
)

from common import argument_session, connection_argument_parser


def parse_args():
    parser = connection_argument_parser()
    parser.add_argument("--min-nodes", type=int, default=6)
    parser.add_argument("--max-nodes", type=int, default=18)
    parser.add_argument(
        "--node-count-file",
        default="/tmp/ring_node_count",
        help="Path written with the chosen ring size; read by modify/verify phases.",
    )
    parser.add_argument("--seed", type=int, default=None)
    return parser.parse_args()


def build_ring(session, num_nodes):
    session.run("MATCH (n) DETACH DELETE n").consume()
    session.run("CREATE INDEX ON :Node(name)").consume()

    for i in range(num_nodes):
        a = str(i)
        b = str((i + 1) % num_nodes)
        session.run(
            """
            MERGE (a:Node {name: $a})
            MERGE (b:Node {name: $b})
            MERGE (a)-[:FRIEND]->(b)
            """,
            a=a,
            b=b,
        ).consume()


def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    num_nodes = random.randint(args.min_nodes, args.max_nodes)

    with argument_session(args) as session:
        build_ring(session, num_nodes)

    with open(args.node_count_file, "w") as f:
        f.write(str(num_nodes))

    print(f"Created ring with {num_nodes} nodes (size written to {args.node_count_file})")


if __name__ == "__main__":
    main()
