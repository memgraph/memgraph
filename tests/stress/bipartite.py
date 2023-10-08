#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

"""
Large bipartite graph stress test.
"""

import atexit
import logging
import multiprocessing
import time

from common import (
    OutputData,
    SessionCache,
    assert_equal,
    batch,
    connection_argument_parser,
    execute_till_success,
    render,
)


def parse_args():
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


log = logging.getLogger(__name__)
args = parse_args()
output_data = OutputData()


atexit.register(SessionCache.cleanup)


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


def traverse_from_u_worker(u):
    """
    Traverses edges starting from an element of U set.
    Traversed labels are: :U -> :V -> :U.
    """
    session = SessionCache.argument_session(args)
    start_time = time.time()
    assert_equal(
        args.u_count * args.v_count - args.v_count,  # cypher morphism
        session.run("MATCH (u1:U {id: %s})-[e1]->(v:V)<-[e2]-(u2:U) " "RETURN count(v) AS cnt" % u).data()[0]["cnt"],
        "Number of traversed edges started " "from U(id:%s) is wrong!. " % u + "Expected: %s Actual: %s",
    )
    end_time = time.time()
    return u, end_time - start_time, "s"


def traverse_from_v_worker(v):
    """
    Traverses edges starting from an element of V set.
    Traversed labels are: :V -> :U -> :V.
    """
    session = SessionCache.argument_session(args)
    start_time = time.time()
    assert_equal(
        args.u_count * args.v_count - args.u_count,  # cypher morphism
        session.run("MATCH (v1:V {id: %s})<-[e1]-(u:U)-[e2]->(v2:V) " "RETURN count(u) AS cnt" % v).data()[0]["cnt"],
        "Number of traversed edges started " "from V(id:%s) is wrong!. " % v + "Expected: %s Actual: %s",
    )
    end_time = time.time()
    return v, end_time - start_time, "s"


def execution_handler():
    """
    Initializes client processes, database and starts the execution.
    """
    # instance cleanup
    session = SessionCache.argument_session(args)
    start_time = time.time()

    # clean existing database
    session.run("MATCH (n) DETACH DELETE n").consume()
    cleanup_end_time = time.time()
    output_data.add_measurement("cleanup_time", cleanup_end_time - start_time)
    log.info("Database is clean.")

    # create indices
    session.run("CREATE INDEX ON :U").consume()
    session.run("CREATE INDEX ON :V").consume()

    # create U vertices
    for b in batch(render("CREATE (:U {{id: {}}})", range(args.u_count)), args.vertex_batch_size):
        session.run(" ".join(b)).consume()
    # create V vertices
    for b in batch(render("CREATE (:V {{id: {}}})", range(args.v_count)), args.vertex_batch_size):
        session.run(" ".join(b)).consume()
    vertices_create_end_time = time.time()
    output_data.add_measurement("vertices_create_time", vertices_create_end_time - cleanup_end_time)
    log.info("All nodes created.")

    # concurrent create execution & tests
    with multiprocessing.Pool(args.worker_count) as p:
        create_edges_start_time = time.time()
        for worker_id, create_time, time_unit, no_failures in p.map(create_u_v_edges, [i for i in range(args.u_count)]):
            log.info("Worker ID: %s; Create time: %s%s Failures: %s" % (worker_id, create_time, time_unit, no_failures))
        create_edges_end_time = time.time()
        output_data.add_measurement("edges_create_time", create_edges_end_time - create_edges_start_time)

        # check total number of edges
        assert_equal(
            args.v_count * args.u_count,
            session.run("MATCH ()-[r]->() " "RETURN count(r) AS cnt").data()[0]["cnt"],
            "Total number of edges isn't correct! Expected: %s Actual: %s",
        )

        # check traversals starting from all elements of U
        traverse_from_u_start_time = time.time()
        for u, traverse_u_time, time_unit in p.map(traverse_from_u_worker, [i for i in range(args.u_count)]):
            log.info("U {id: %s} %s%s" % (u, traverse_u_time, time_unit))
        traverse_from_u_end_time = time.time()
        output_data.add_measurement("traverse_from_u_time", traverse_from_u_end_time - traverse_from_u_start_time)

        # check traversals starting from all elements of V
        traverse_from_v_start_time = time.time()
        for v, traverse_v_time, time_unit in p.map(traverse_from_v_worker, [i for i in range(args.v_count)]):
            log.info("V {id: %s} %s%s" % (v, traverse_v_time, time_unit))
        traverse_from_v_end_time = time.time()
        output_data.add_measurement("traverse_from_v_time", traverse_from_v_end_time - traverse_from_v_start_time)

    # check total number of vertices
    assert_equal(
        args.v_count + args.u_count,
        session.run("MATCH (n) RETURN count(n) AS cnt").data()[0]["cnt"],
        "Total number of vertices isn't correct! Expected: %s Actual: %s",
    )

    # check total number of edges
    assert_equal(
        args.v_count * args.u_count,
        session.run("MATCH ()-[r]->() RETURN count(r) AS cnt").data()[0]["cnt"],
        "Total number of edges isn't correct! Expected: %s Actual: %s",
    )

    end_time = time.time()
    output_data.add_measurement("total_execution_time", end_time - start_time)


if __name__ == "__main__":
    logging.basicConfig(level=args.logging)
    if args.logging != "DEBUG":
        logging.getLogger("neo4j").setLevel(logging.WARNING)
    output_data.add_status("stress_test_name", "bipartite")
    output_data.add_status("number_of_vertices", args.u_count + args.v_count)
    output_data.add_status("number_of_edges", args.u_count * args.v_count)
    output_data.add_status("vertex_batch_size", args.vertex_batch_size)
    output_data.add_status("edge_batching", args.edge_batching)
    output_data.add_status("edge_batch_size", args.edge_batch_size)
    execution_handler()
    if args.logging in ["DEBUG", "INFO"]:
        output_data.dump()
