#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2023 Memgraph Ltd.
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
Large bipartite graph stress test.
"""

import logging
import multiprocessing
import time
from argparse import Namespace as Args
from functools import wraps
from typing import Any, Callable

from common import (
    OutputData,
    connection_argument_parser,
    execute_till_success_gqlalchemy,
)
from gqlalchemy import Memgraph

log = logging.getLogger(__name__)
output_data = OutputData()


def parse_args() -> Args:
    """
    Parses user arguments

    :return: parsed arguments
    """
    parser = connection_argument_parser()
    parser.add_argument(
        "--worker-count", type=int, default=multiprocessing.cpu_count(), help="Number of concurrent workers."
    )
    parser.add_argument(
        "--logging", default="INFO", choices=["INFO", "DEBUG", "WARNING", "ERROR"], help="Logging level"
    )
    parser.add_argument("--u-count", type=int, default=100, help="Size of U set in the bipartite graph.")
    parser.add_argument("--v-count", type=int, default=100, help="Size of V set in the bipartite graph.")
    parser.add_argument("--vertex-batch-size", type=int, default=100, help="Create vertices in batches of this size.")
    parser.add_argument("--edge-batching", action="store_true", help="Create edges in batches.")
    parser.add_argument(
        "--edge-batch-size",
        type=int,
        default=100,
        help="Number of edges in a batch when edges " "are created in batches.",
    )
    parser.add_argument("--isolation-level", type=str, required=True, help="Database isolation level.")
    parser.add_argument("--storage-mode", type=str, required=True, help="Database storage mode.")
    return parser.parse_args()


def timed_function(name) -> Callable:
    def actual_decorator(func) -> Callable:
        @wraps(func)
        def timed_wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            output_data.add_measurement(name, end_time - start_time)
            return result

        return timed_wrapper

    return actual_decorator


@timed_function("cleanup_time")
def clean_database() -> None:
    memgraph = Memgraph()
    memgraph.execute("MATCH (n) DETACH DELETE n")


def create_indices() -> None:
    memgraph = Memgraph()
    memgraph.execute("CREATE INDEX ON :U")
    memgraph.execute("CREATE INDEX ON :V")


@timed_function("node_creation")
def create_nodes(args: Args) -> None:
    memgraph = Memgraph()
    memgraph.execute(" ".join([f"CREATE (:U{{id: {i}}})" for i in range(args.u_count)]))
    memgraph.execute(" ".join([f"CREATE (:V{{id: {i}}})" for i in range(args.v_count)]))


def _create_edges(u):
    start_time = time.time()
    memgraph = Memgraph()
    match_u = "MATCH (u:U {id: %d})" % u

    _, no_failures = execute_till_success_gqlalchemy(memgraph, match_u + " MATCH (v:V) CREATE (u)-[:R]->(v)")

    end_time = time.time()
    return u, end_time - start_time, no_failures


@timed_function("edge_creation")
def create_edges(args):
    # concurrent create execution
    with multiprocessing.Pool(args.worker_count) as p:
        for worker_id, create_time, no_failures in p.map(_create_edges, [i for i in range(args.u_count)]):
            log.info(f"Worker ID: {worker_id}; Create time: {create_time}s with {no_failures} failures.")


@timed_function("total_execution_time")
def execution_handler(args: Args) -> None:
    clean_database()
    log.info("Database is clean.")

    create_indices()
    create_nodes(args)
    create_edges(args)


if __name__ == "__main__":
    args = parse_args()

    logging.basicConfig(level=args.logging)
    execution_handler(args)
    if args.logging in ["DEBUG", "INFO"]:
        output_data.dump()
