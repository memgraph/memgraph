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
Ring rotation stress workload (Memgraph port of antithesishq/dgraph
ring_test, without the Antithesis SDK).

Three phases, executed as separate subprocesses so the modify phase can
run many writers in parallel:

  1. first_make_ring.py   build an N-node directed cycle
  2. modify_ring.py x P   P parallel workers, each doing M random
                          triplet swaps; after every commit the worker
                          re-reads all edges and re-asserts the ring
  3. finally_test_ring.py walk the ring from every node and assert it
                          closes in exactly N hops

Invoked by the stress runner via standalone/native/workloads.yaml, but
also runnable directly:

  ./workload.py --endpoint 127.0.0.1:7687 --parallelism 8
"""

import argparse
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--endpoint", default=os.environ.get("ENDPOINT", "127.0.0.1:7687"))
    parser.add_argument("--username", default="neo4j")
    parser.add_argument("--password", default="1234")
    parser.add_argument("--parallelism", type=int, default=int(os.environ.get("PARALLELISM", 4)))
    parser.add_argument("--min-nodes", type=int, default=6)
    parser.add_argument("--max-nodes", type=int, default=18)
    parser.add_argument("--min-swaps", type=int, default=int(os.environ.get("NUM_SWAPS_MIN", 75)))
    parser.add_argument("--max-swaps", type=int, default=int(os.environ.get("NUM_SWAPS_MAX", 150)))
    parser.add_argument("--node-count-file", default="/tmp/ring_node_count")
    parser.add_argument("--seed", type=int, default=None)
    # The stress runner passes these; we accept and ignore.
    parser.add_argument("--worker-count", type=int, default=None)
    parser.add_argument("--logging", default=None)
    return parser.parse_args()


def _conn_args(args):
    return ["--endpoint", args.endpoint, "--username", args.username, "--password", args.password]


def _redact(cmd):
    redacted = list(cmd)
    for i, tok in enumerate(redacted):
        if tok in ("--username", "--password") and i + 1 < len(redacted):
            redacted[i + 1] = "***"
    return redacted


def run(cmd):
    print(f"+ {' '.join(_redact(cmd))}", flush=True)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(f"FAILED: {' '.join(_redact(cmd))} (rc={result.returncode})")


def main():
    args = parse_args()

    print(f"==> Phase 1: build ring [{args.min_nodes}, {args.max_nodes}]")
    cmd = (
        [sys.executable, "-u", os.path.join(SCRIPT_DIR, "first_make_ring.py")]
        + _conn_args(args)
        + [
            "--node-count-file",
            args.node_count_file,
            "--min-nodes",
            str(args.min_nodes),
            "--max-nodes",
            str(args.max_nodes),
        ]
    )
    if args.seed is not None:
        cmd += ["--seed", str(args.seed)]
    run(cmd)

    parallelism = max(1, args.parallelism)
    print(f"==> Phase 2: {parallelism} parallel modify workers " f"({args.min_swaps}..{args.max_swaps} swaps each)")
    procs = []
    for _ in range(parallelism):
        cmd = (
            [sys.executable, "-u", os.path.join(SCRIPT_DIR, "modify_ring.py")]
            + _conn_args(args)
            + [
                "--node-count-file",
                args.node_count_file,
                "--min-swaps",
                str(args.min_swaps),
                "--max-swaps",
                str(args.max_swaps),
            ]
        )
        procs.append(subprocess.Popen(cmd))

    failed = False
    for p in procs:
        if p.wait() != 0:
            failed = True
    if failed:
        sys.exit("At least one modify_ring worker failed")

    print("==> Phase 3: verify ring")
    cmd = (
        [sys.executable, "-u", os.path.join(SCRIPT_DIR, "finally_test_ring.py")]
        + _conn_args(args)
        + [
            "--node-count-file",
            args.node_count_file,
        ]
    )
    run(cmd)

    print("==> Ring stress workload complete")


if __name__ == "__main__":
    main()
