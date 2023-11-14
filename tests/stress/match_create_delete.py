#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Busi ness Source
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
import random
import time
from argparse import Namespace as Args
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Tuple

from common import (
    OutputData,
    SessionCache,
    connection_argument_parser,
    execute_till_success,
)

log = logging.getLogger(__name__)
output_data = OutputData()

NUMBER_NODES_IN_CHAIN = 4
CREATE_FUNCTION = "CREATE"
DELETE_FUNCTION = "DELETE"
MATCH_FUNCTION = "MATCH"


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
    parser.add_argument("--repetition-count", type=int, default=1000, help="Number of times to perform the action")
    parser.add_argument("--isolation-level", type=str, required=True, help="Database isolation level.")
    parser.add_argument("--storage-mode", type=str, required=True, help="Database storage mode.")

    return parser.parse_args()


args = parse_args()


@dataclass
class Worker:
    """
    Class that performs a function defined in the `type` argument

    Args:
    type - either `CREATE` or `DELETE`, signifying the function that's going to be performed
        by the worker
    id - worker id
    total_worker_cnt - total number of workers for reference
    repetition_count - number of times to perform the worker action
    sleep_sec - float for subsecond sleeping between two subsequent actions
    """

    type: str
    id: int
    total_worker_cnt: int
    repetition_count: int
    sleep_sec: float


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
    execute_till_success(session, "CREATE INDEX ON :Node(prop)")
    execute_till_success(session, "CREATE INDEX ON :Supernode")
    execute_till_success(session, "CREATE INDEX ON :Supernode(prop)")


def setup_database_mode() -> None:
    session = SessionCache.argument_session(args)
    execute_till_success(session, f"STORAGE MODE {args.storage_mode}")
    execute_till_success(session, f"SET GLOBAL TRANSACTION ISOLATION LEVEL {args.isolation_level}")


def execute_function(worker: Worker) -> Worker:
    """
    Executes the function based on the worker type
    """
    if worker.type == CREATE_FUNCTION:
        run_writer(worker.total_worker_cnt, worker.repetition_count, worker.sleep_sec, worker.id)
        return worker

    if worker.type == DELETE_FUNCTION:
        run_deleter(worker.total_worker_cnt, worker.repetition_count, worker.sleep_sec)
        return worker

    if worker.type == MATCH_FUNCTION:
        run_matcher(worker.total_worker_cnt, worker.repetition_count, worker.sleep_sec)
        return worker

    raise Exception("Worker function not recognized, raising exception!")


def run_writer(total_workers_cnt: int, repetition_count: int, sleep_sec: float, worker_id: int) -> int:
    """
    Creating nodes connected to a supernode
    """
    session = SessionCache.argument_session(args)

    def create():
        try:
            number = random.randint(1, 1000000)
            execute_till_success(
                session,
                f"FOREACH (i in range(1, 200000) | CREATE (n:Node {{prop: {number}}}))",
            )
            execute_till_success(
                session,
                f"CREATE (:Supernode {{prop: {number}}})",
            )
            execute_till_success(
                session, f"MATCH (n:Node {{prop: {number}}}), (m:Supernode {{prop: {number}}}) CREATE (n)-[:TYPE]->(m)"
            )
        except Exception as ex:
            pass

    curr_repetition = 0

    while curr_repetition < repetition_count:
        create()
        time.sleep(sleep_sec)
        log.info(f"Worker {worker_id} created chain in iteration {curr_repetition}")
        curr_repetition += 1


def run_deleter(total_workers_cnt: int, repetition_count: int, sleep_sec: float) -> None:
    """
    Periodic deletion of an arbitrary subgraph in the graph
    """
    session = SessionCache.argument_session(args)

    def delete_part_of_graph():
        try:
            maybe_prop = execute_till_success(session, "MATCH (n) WITH n.prop AS prop LIMIT 1 RETURN prop")[0]
            if len(maybe_prop):
                prop = maybe_prop[0]["prop"]
                execute_till_success(session, f"MATCH (n {{prop: {prop}}})-[r]->() DELETE r")
                execute_till_success(session, f"MATCH (n {{prop: {prop}}}) DETACH DELETE n")
        except Exception as ex:
            log.info(f"Worker failed to delete the chain with id {id}")
            pass

    curr_repetition = 0
    while curr_repetition < repetition_count:
        delete_part_of_graph()
        time.sleep(sleep_sec)
        curr_repetition += 1


def run_matcher(total_workers_cnt: int, repetition_count: int, sleep_sec: float) -> None:
    """
    Matching edges and returning the count.
    """
    session = SessionCache.argument_session(args)

    def delete_part_of_graph(id: int):
        try:
            execute_till_success(session, f"MATCH ()-[r]->(m) RETURN COUNT(r)")
            log.info(f"Matching done {id}")
            time.sleep(sleep_sec)
        except Exception as ex:
            log.info(f"Worker failed to match with id {id}")
            time.sleep(sleep_sec)
            pass

    curr_repetition = 0
    while curr_repetition < repetition_count:
        random_part_of_graph = random.randint(0, total_workers_cnt - 1)
        delete_part_of_graph(random_part_of_graph)
        time.sleep(sleep_sec)
        curr_repetition += 1


@timed_function("total_execution_time")
def execution_handler() -> None:
    clean_database()
    log.info("Database is clean.")

    setup_database_mode()

    create_indices()

    rep_count = args.repetition_count
    sleep_sec = 0.5

    workers = [
        Worker(CREATE_FUNCTION, 0, 3, rep_count, sleep_sec),
        Worker(DELETE_FUNCTION, 1, 3, rep_count, sleep_sec),
        Worker(MATCH_FUNCTION, 2, 3, rep_count, sleep_sec),
        Worker(CREATE_FUNCTION, 3, 3, rep_count, sleep_sec),
        Worker(DELETE_FUNCTION, 4, 3, rep_count, sleep_sec),
        Worker(MATCH_FUNCTION, 5, 3, rep_count, sleep_sec),
        Worker(CREATE_FUNCTION, 6, 3, rep_count, sleep_sec),
        Worker(DELETE_FUNCTION, 7, 3, rep_count, sleep_sec),
        Worker(MATCH_FUNCTION, 8, 3, rep_count, sleep_sec),
    ]

    with multiprocessing.Pool(processes=args.worker_count) as p:
        for worker in p.map(execute_function, workers):
            print(f"Worker {worker.type} finished!")


if __name__ == "__main__":
    logging.basicConfig(level=args.logging)
    execution_handler()
    if args.logging in ["DEBUG", "INFO"]:
        output_data.dump()
