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
Stress test for monitoring how memory tracker behaves when
there is lot of node creation and deletions compared
to RES memory usage.
"""

import atexit
import logging
import multiprocessing
import time
from argparse import Namespace as Args
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

from common import (
    OutputData,
    SessionCache,
    connection_argument_parser,
    execute_till_success,
    try_execute,
)

log = logging.getLogger(__name__)
output_data = OutputData()


class Constants:
    CREATE_FUNCTION = "CREATE"


atexit.register(SessionCache.cleanup)

MEMORY_LIMIT = 2048


def parse_args() -> Args:
    """
    Parses user arguments

    :return: parsed arguments
    """
    parser = connection_argument_parser()
    parser.add_argument("--worker-count", type=int, default=5, help="Number of concurrent workers.")
    parser.add_argument(
        "--logging", default="INFO", choices=["INFO", "DEBUG", "WARNING", "ERROR"], help="Logging level"
    )
    parser.add_argument("--repetition-count", type=int, default=1000, help="Number of times to perform the action")
    parser.add_argument("--isolation-level", type=str, required=True, help="Database isolation level.")
    parser.add_argument("--storage-mode", type=str, required=True, help="Database storage mode.")

    return parser.parse_args()


# Global variables


args = parse_args()

# Difference between memory RES and memory tracker on
# Memgraph start.
# Due to various other things which are included in RES
# there is difference of ~30MBs initially.
initial_diff = 0


@dataclass
class Worker:
    """
    Class that performs a function defined in the `type` argument.

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


def setup_database_mode() -> None:
    session = SessionCache.argument_session(args)
    execute_till_success(session, f"STORAGE MODE {args.storage_mode}")
    execute_till_success(session, f"SET GLOBAL TRANSACTION ISOLATION LEVEL {args.isolation_level}")


def get_tracker_data(session) -> Optional[float]:
    def parse_data(allocated: str) -> float:
        num = 0
        if "KiB" in allocated or "MiB" in allocated or "GiB" in allocated or "TiB" in allocated:
            num = float(allocated[:-3])
        else:
            num = float(allocated[-1])

        if "KiB" in allocated:
            return num / 1024
        if "MiB" in allocated:
            return num
        if "GiB" in allocated:
            return num * 1024
        else:
            return num * 1024 * 1024

    def isolate_value(data: List[Dict[str, Any]], key: str) -> Optional[str]:
        for dict in data:
            if dict["storage info"] == key:
                return dict["value"]
        return None

    try:
        data, _ = try_execute(session, f"SHOW STORAGE INFO")
        memory_tracker_data = isolate_value(data, "memory_tracked")

        return parse_data(memory_tracker_data)

    except Exception as ex:
        log.info(f"Get storage info failed with error", ex)
        return None


def run_writer(repetition_count: int, sleep_sec: float, worker_id: int) -> int:
    """
    This writer creates lot of nodes on each write.
    Also it checks that query failed if memory limit is tried to be broken
    """

    session = SessionCache.argument_session(args)

    def create() -> bool:
        """
        Returns True if done, False if needs to continue
        """
        memory_tracker_data_before_start = get_tracker_data(session)
        should_fail = memory_tracker_data_before_start >= 2048
        failed = False
        try:
            try_execute(
                session,
                f"FOREACH (i in range(1,10000) | CREATE (:Node {{prop:'big string or something like that'}}))",
            )
        except Exception as ex:
            failed = True
            output = str(ex)
            log.info("Exception in create", output)
            assert "Memory limit exceeded!" in output

        if should_fail:
            assert failed, "Query should have failed"
            return False
        return True

    curr_repetition = 0

    while curr_repetition < repetition_count:
        log.info(f"Worker {worker_id} started iteration {curr_repetition}")

        should_continue = create()

        if not should_continue:
            return True

        time.sleep(sleep_sec)
        log.info(f"Worker {worker_id} created chain in iteration {curr_repetition}")

        curr_repetition += 1


def execute_function(worker: Worker) -> Worker:
    """
    Executes the function based on the worker type
    """
    if worker.type == Constants.CREATE_FUNCTION:
        run_writer(worker.repetition_count, worker.sleep_sec, worker.id)
        log.info(f"Worker {worker.type} finished!")
        return worker

    raise Exception("Worker function not recognized, raising exception!")


@timed_function("total_execution_time")
def execution_handler() -> None:
    clean_database()
    log.info("Database is clean.")

    setup_database_mode()

    create_indices()

    worker_count = args.worker_count
    rep_count = args.repetition_count

    workers = []
    for i in range(worker_count):
        workers.append(Worker(Constants.CREATE_FUNCTION, i, worker_count, rep_count, 0.1))

    with multiprocessing.Pool(processes=worker_count) as p:
        for worker in p.map(execute_function, workers):
            log.info(f"Worker {worker.type} finished!")


if __name__ == "__main__":
    logging.basicConfig(level=args.logging)

    execution_handler()
    if args.logging in ["DEBUG", "INFO"]:
        output_data.dump()
