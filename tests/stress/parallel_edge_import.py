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
Parallel edge import stress test.
"""

import atexit
import logging
import multiprocessing
import random
import time
from argparse import Namespace as Args
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable

from common import (
    OutputData,
    SessionCache,
    connection_argument_parser,
    execute_till_success,
)

log = logging.getLogger(__name__)
output_data = OutputData()


NODE_IMPORT_FUNCTION = "NODE_IMPORT"
EDGE_IMPORT_FUNCTION = "EDGE_IMPORT"
NODE_COUNT = 100


atexit.register(SessionCache.cleanup)


def parse_args() -> Args:
    """
    Parses user arguments

    :return: parsed arguments
    """
    parser = connection_argument_parser()
    parser.add_argument("--worker-count", type=int, default=4, help="Number of concurrent workers.")
    parser.add_argument(
        "--logging", default="INFO", choices=["INFO", "DEBUG", "WARNING", "ERROR"], help="Logging level"
    )
    parser.add_argument(
        "--edge-import-per-transaction-count", type=int, default=50, help="Number of edges imported in one query"
    )
    parser.add_argument("--repetition-count", type=int, default=10, help="Number of repetitions for every worker")
    parser.add_argument("--isolation-level", type=str, required=True, help="Database isolation level.")
    parser.add_argument("--storage-mode", type=str, required=True, help="Database storage mode.")

    return parser.parse_args()


args = parse_args()


@dataclass
class Worker:
    """
    Args:
    type: str - string representation of the Worker function
        * NODE_IMPORT -> import of nodes
        * EDGE_IMPORT -> parallel import of edges
    id: int - worker id
    number_of_edges_to_import: int - number of edges for one repetition
    repetition_count: int - number of repetitions
    """

    type: str
    id: int
    number_of_edges_to_import: int
    repetition_count: int


def timed_function(name) -> Callable:
    """
    Times performed function
    """

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
    session = SessionCache.argument_session(args)
    execute_till_success(session, "MATCH (n) DETACH DELETE n")


def create_indices() -> None:
    session = SessionCache.argument_session(args)
    execute_till_success(session, "CREATE INDEX ON :Node")
    execute_till_success(session, "CREATE INDEX ON :Node(id)")


def setup_database_mode() -> None:
    session = SessionCache.argument_session(args)
    execute_till_success(session, f"STORAGE MODE {args.storage_mode}")
    execute_till_success(session, f"SET GLOBAL TRANSACTION ISOLATION LEVEL {args.isolation_level}")


def execute_function(worker: Worker) -> Worker:
    if worker.type == NODE_IMPORT_FUNCTION:
        run_node_import(worker.id)
        return worker
    if worker.type == EDGE_IMPORT_FUNCTION:
        run_edge_import(worker.id, worker.number_of_edges_to_import, worker.repetition_count)
        return worker

    raise Exception("Worker function not recognized for parallel edge import, raising exception!")


def run_node_import(worker_id: int) -> None:
    session = SessionCache.argument_session(args)
    execute_till_success(session, f"FOREACH (i in range(1, {NODE_COUNT}) | MERGE (:Node {{id: i}}))")

    def verify():
        count = execute_till_success(session, f"MATCH (n) RETURN COUNT(n) AS cnt")[0][0]["cnt"]
        log.info(f"Verifying node import count from {worker_id}")
        assert count == NODE_COUNT

    verify()


def run_edge_import(worker_id: int, edge_number_to_import: int, repetition_count: int) -> None:
    session = SessionCache.argument_session(args)

    for _ in range(repetition_count):
        random_ids = [
            (random.randint(1, NODE_COUNT), random.randint(1, NODE_COUNT)) for x in range(edge_number_to_import)
        ]
        stringified_ids = [f"[{x[0]},{x[1]}]" for x in random_ids]
        unwind = f"UNWIND [{','.join(stringified_ids)}] AS from_to"

        execute_till_success(
            session,
            f"{unwind} MATCH (n:Node {{id: from_to[0]}}) MATCH (m:Node {{id: from_to[1]}}) CREATE (n)-[:TYPE]->(m)",
            1,
        )


def final_verification():
    session = SessionCache.argument_session(args)
    total_edge_import_size = args.worker_count * args.repetition_count * args.edge_import_per_transaction_count

    count = execute_till_success(session, f"MATCH (n)-[r]->(m) RETURN COUNT(*) AS cnt")[0][0]["cnt"]
    log.info(f"Verifying if total number of imported edges matches...")

    assert count == total_edge_import_size

    log.info(f"Total number of edges matches the numbers!")


@timed_function("total_execution_time")
def execution_handler() -> None:
    clean_database()
    log.info("Database is clean.")

    setup_database_mode()

    create_indices()

    worker_count = args.worker_count

    node_import_worker = Worker(NODE_IMPORT_FUNCTION, 0, 0, 1)
    edge_import_workers = [
        Worker(EDGE_IMPORT_FUNCTION, x + 1, args.edge_import_per_transaction_count, args.repetition_count)
        for x in range(worker_count)
    ]

    execute_function(node_import_worker)
    with multiprocessing.Pool(processes=args.worker_count) as p:
        for worker in p.map(execute_function, edge_import_workers):
            print(f"Worker {worker.id} finished!")

    final_verification()


if __name__ == "__main__":
    logging.basicConfig(level=args.logging)
    execution_handler()
    if args.logging in ["DEBUG", "INFO"]:
        output_data.dump()
