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

from common import (
    OutputData,
    SessionCache,
    assert_equal,
    batch,
    connection_argument_parser,
    execute_till_success,
    render,
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


def create_u_v_edges(u):
    """
    Creates nodes and checks that all nodes were created.
    create edges from one vertex in U set to all vertex of V set

    :param worker_id: worker id

    :return: tuple (worker_id, create execution time, time unit)
    """
    start_time = time.time()
    session = SessionCache.argument_session(args)
    no_failures = 0
    match_u = "MATCH (u:U {id: %d})" % u
    if args.edge_batching:
        # TODO: try to randomize execution, the execution time should
        # be smaller, add randomize flag
        for v_id_batch in batch(range(args.v_count), args.edge_batch_size):
            match_v = render(" MATCH (v{0}:V {{id: {0}}})", v_id_batch)
            create_u = render(" CREATE (u)-[:R]->(v{0})", v_id_batch)
            query = match_u + "".join(match_v) + "".join(create_u)
            no_failures += execute_till_success(session, query)[1]
    else:
        no_failures += execute_till_success(session, match_u + " MATCH (v:V) CREATE (u)-[:R]->(v)")[1]

    end_time = time.time()
    return u, end_time - start_time, "s", no_failures


def timed_function(name):
    def actual_decorator(func):
        @wraps(func)
        def timed_wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            output_data.add_measurement(name, end_time - start_time)
            return result

        return timed_wrapper

    return actual_decorator


@timed_function("cleanup_time")
def clean_database():
    memgraph = Memgraph()
    memgraph.execute("MATCH (n) DETACH DELETE n")


@timed_function("total_execution_time")
def execution_handler(args: Args):
    clean_database()
    log.info("Database is clean.")
    # # create indices
    # session.run('CREATE INDEX ON :U').consume()
    # session.run('CREATE INDEX ON :V').consume()

    # # create U vertices
    # for b in batch(render('CREATE (:U {{id: {}}})', range(args.u_count)),
    #                args.vertex_batch_size):
    #     session.run(" ".join(b)).consume()
    # # create V vertices
    # for b in batch(render('CREATE (:V {{id: {}}})', range(args.v_count)),
    #                args.vertex_batch_size):
    #     session.run(" ".join(b)).consume()
    # vertices_create_end_time = time.time()
    # output_data.add_measurement(
    #     'vertices_create_time',
    #     vertices_create_end_time - cleanup_end_time)
    # log.info("All nodes created.")

    # # concurrent create execution & tests
    # with multiprocessing.Pool(args.worker_count) as p:
    #     create_edges_start_time = time.time()
    #     for worker_id, create_time, time_unit, no_failures in \
    #             p.map(create_u_v_edges, [i for i in range(args.u_count)]):
    #         log.info('Worker ID: %s; Create time: %s%s Failures: %s' %
    #                  (worker_id, create_time, time_unit, no_failures))
    #     create_edges_end_time = time.time()
    #     output_data.add_measurement(
    #         'edges_create_time',
    #         create_edges_end_time - create_edges_start_time)


if __name__ == "__main__":
    args = parse_args()

    logging.basicConfig(level=args.logging)
    execution_handler(args)
    if args.logging in ["DEBUG", "INFO"]:
        output_data.dump()
