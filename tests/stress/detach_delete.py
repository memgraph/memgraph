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
    execute_till_success(session, "CREATE INDEX ON :Node(id)")


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

    raise Exception("Worker function not recognized, raising exception!")


def run_writer(total_workers_cnt: int, repetition_count: int, sleep_sec: float, worker_id: int) -> int:
    """
    This writer creates a chain and wants to verify after each action if the action he performed is
    a valid graph. A graph is valid if the number of nodes is preserved, and the chain is either
    not present or present completely.
    """
    session = SessionCache.argument_session(args)

    def create():
        try:
            execute_till_success(
                session,
                f"MERGE (:Node{worker_id} {{id: 1}})-[:REL]-(:Node{worker_id} {{id: 2}})-[:REL]-(:Node{worker_id} {{id: 3}})-[:REL]-(:Node{worker_id} {{id: 4}})",
            )
        except Exception as ex:
            pass

    def verify() -> Tuple[bool, int]:
        # We always create X nodes and therefore the number of nodes needs to be always a fraction of X
        count = execute_till_success(session, f"MATCH (n) RETURN COUNT(n) AS cnt")[0][0]["cnt"]
        log.info(f"Worker {worker_id} verified graph count {count} in repetition {curr_repetition}")

        assert count <= total_workers_cnt * NUMBER_NODES_IN_CHAIN and count % NUMBER_NODES_IN_CHAIN == 0

        ids = execute_till_success(
            session,
            f"MATCH (n:Node{worker_id} {{id: 1}})-->(m)-->(o)-->(p) RETURN n.id AS id1, m.id AS id2, o.id AS id3, p.id AS id4",
        )[0]

        if len(ids):
            result = ids[0]
            assert "id1" in result and "id2" in result and "id3" in result and "id4" in result
            assert result["id1"] == 1 and result["id2"] == 2 and result["id3"] == 3 and result["id4"] == 4
            log.info(f"Worker {worker_id} verified graph chain is valid in repetition {curr_repetition}")
        else:
            log.info(f"Worker {worker_id} does not have a chain in repetition {repetition_count}")

    curr_repetition = 0

    while curr_repetition < repetition_count:
        log.info(f"Worker {worker_id} started iteration {curr_repetition}")
        create()
        time.sleep(sleep_sec)
        log.info(f"Worker {worker_id} created chain in iteration {curr_repetition}")

        verify()

        curr_repetition += 1


def run_deleter(total_workers_cnt: int, repetition_count: int, sleep_sec: float) -> None:
    """
    Periodic deletion of an arbitrary chain in the graph
    """
    session = SessionCache.argument_session(args)

    def delete_part_of_graph(id: int):
        try:
            execute_till_success(session, f"MATCH (n:Node{id}) DETACH DELETE n")
            log.info(f"Worker deleted chain with nodes of id {id}")
        except Exception as ex:
            log.info(f"Worker failed to delete the chain with id {id}")
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

    worker_count = args.worker_count
    rep_count = args.repetition_count

    workers = [Worker(CREATE_FUNCTION, x, worker_count - 1, rep_count, 0.2) for x in range(worker_count - 1)]
    workers.append(Worker(DELETE_FUNCTION, -1, worker_count - 1, rep_count, 0.15))

    with multiprocessing.Pool(processes=args.worker_count) as p:
        for worker in p.map(execute_function, workers):
            print(f"Worker {worker.type} finished!")


if __name__ == "__main__":
    logging.basicConfig(level=args.logging)
    execution_handler()
    if args.logging in ["DEBUG", "INFO"]:
        output_data.dump()
