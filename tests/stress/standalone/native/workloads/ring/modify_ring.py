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
Phase 2 of the ring stress test (Memgraph port of antithesishq/dgraph
ring_test/modify_ring.py, without the Antithesis SDK).

Repeatedly picks two random triplets (prev -> curr -> next) in the ring
and atomically swaps the two `curr` nodes while keeping the ring closed.
"""

import os
import random
import sys
import time

sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")),
)

from common import argument_session, connection_argument_parser
from neo4j.exceptions import TransientError


def parse_args():
    parser = connection_argument_parser()
    parser.add_argument("--node-count-file", default="/tmp/ring_node_count")
    parser.add_argument("--min-swaps", type=int, default=int(os.environ.get("NUM_SWAPS_MIN", 75)))
    parser.add_argument("--max-swaps", type=int, default=int(os.environ.get("NUM_SWAPS_MAX", 150)))
    parser.add_argument("--max-retries", type=int, default=30)
    return parser.parse_args()


def check_if_ring(links):
    """Returns True iff `links` (list of (u, v) pairs) forms a single cycle."""
    if not links:
        return False
    by_src = {u: v for (u, v) in links}
    if len(by_src) != len(links):
        return False  # duplicate source -> not a simple cycle
    start = links[0][0]
    node = start
    for _ in range(len(links) - 1):
        node = by_src.get(node)
        if node is None:
            return False
    return by_src.get(node) == start


def local_modify(links, dels, makes):
    """Apply dels/makes locally and check the ring is still intact. Mirrors dgraph."""
    links_copy = list(links)
    if not check_if_ring(links_copy):
        return "Original Ring Broken", links_copy
    for u, v in dels:
        if (u, v) in links_copy:
            links_copy.remove((u, v))
    for u, v in makes:
        links_copy.append((u, v))
    if len(links_copy) != len(links):
        return "Txn duped links", links_copy
    if not check_if_ring(links_copy):
        return "Txn Broke Ring", links_copy
    return "Clean Swap", links_copy


def links_to_dot(links):
    return ";".join(f"{u}->{v}" for u, v in links)


def swap_triplets_atomic(session, index0, index1, num_nodes, max_retries):
    """Atomically swap the `curr` nodes of triplets index0 and index1.

    Uses the same a1_hat / b3_hat substitution trick as the dgraph test so the
    adjacent (Y, Z are neighbors) case collapses correctly via set-dedup of
    new edges.
    """
    assert index0 != index1
    assert num_nodes >= 3

    last_err = None
    for _ in range(max_retries):
        tx = session.begin_transaction()
        try:
            triplets_result = tx.run(
                """
                MATCH (prev:Node)-[:FRIEND]->(curr:Node)-[:FRIEND]->(next:Node)
                RETURN id(prev) AS prev_id, id(curr) AS curr_id, id(next) AS next_id
                ORDER BY toInteger(curr.name)
                """
            )
            triplets = [r.data() for r in triplets_result]

            links = set()
            for t in triplets:
                links.add((t["prev_id"], t["curr_id"]))
                links.add((t["curr_id"], t["next_id"]))
            assert check_if_ring(list(links)), f"Ring broken before commit: {links_to_dot(links)}"

            if index0 >= len(triplets) or index1 >= len(triplets):
                raise IndexError(f"Triplet index out of bounds: {index0}, {index1} / {len(triplets)}")

            t1, t2 = triplets[index0], triplets[index1]
            a1, a2, a3 = t1["prev_id"], t1["curr_id"], t1["next_id"]
            b1, b2, b3 = t2["prev_id"], t2["curr_id"], t2["next_id"]

            a1_hat = a2 if a1 == b2 else a1
            b3_hat = b2 if a2 == b3 else b3
            a3_hat = a2 if a3 == b2 else a3
            b1_hat = b2 if a2 == b1 else b1

            del_edges = [(a1, a2), (a2, a3), (b1, b2), (b2, b3)]
            new_edges = []
            seen = set()
            for u, v in [(a1_hat, b2), (b2, a3_hat), (b1_hat, a2), (a2, b3_hat)]:
                if (u, v) not in seen:
                    seen.add((u, v))
                    new_edges.append((u, v))

            for u, v in del_edges:
                tx.run(
                    "MATCH (a:Node)-[r:FRIEND]->(b:Node) WHERE id(a) = $u AND id(b) = $v DELETE r",
                    u=u,
                    v=v,
                )
            for u, v in new_edges:
                tx.run(
                    """
                    MATCH (a:Node) WHERE id(a) = $u
                    MATCH (b:Node) WHERE id(b) = $v
                    MERGE (a)-[:FRIEND]->(b)
                    """,
                    u=u,
                    v=v,
                )

            tx.commit()

            post_pairs = [
                (r["u"], r["v"])
                for r in session.run("MATCH (a:Node)-[:FRIEND]->(b:Node) RETURN id(a) AS u, id(b) AS v").data()
            ]
            assert check_if_ring(post_pairs), (
                f"Ring broken after commit: {links_to_dot(post_pairs)} " f"| DEL: {del_edges} | MAKE: {new_edges}"
            )

            mod_msg, local_links = local_modify(list(links), del_edges, new_edges)
            print(
                f"START: {links_to_dot(links)} | DEL: {del_edges} | MAKE: {new_edges} | "
                f"{mod_msg}: {links_to_dot(local_links)}"
            )

            return

        except TransientError as e:
            last_err = e
            try:
                tx.rollback()
            except Exception:
                pass
            time.sleep(1)

    raise RuntimeError(f"Failed to commit after {max_retries} retries (last error: {last_err})")


def simulate_transactions(args):
    with open(args.node_count_file) as f:
        num_nodes = int(f.read())
    assert num_nodes >= 3, "Ring needs at least 3 nodes"

    num_swaps = random.randint(args.min_swaps, args.max_swaps)

    with argument_session(args) as session:
        for _ in range(num_swaps):
            i0 = random.randrange(num_nodes)
            choices = list(range(num_nodes))
            choices.remove(i0)
            i1 = random.choice(choices)
            swap_triplets_atomic(session, i0, i1, num_nodes, args.max_retries)


if __name__ == "__main__":
    simulate_transactions(parse_args())
