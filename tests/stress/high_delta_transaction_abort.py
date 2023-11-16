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
    parser.add_argument("--edge-size", type=int, default=2000000, help="Number of edges to create.")
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
    """

    type: str
    id: int


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


def create_data() -> None:
    edge_size = args.edge_size
    session = SessionCache.argument_session(args)
    print("Creating supernode")
    execute_till_success(session, "CREATE (n:Supernode {prop: 1});", 1)
    for i in range(20):
        print(f"Creating dataset {i}/20")
        execute_till_success(
            session, f"FOREACH (i in range(1, {int(edge_size / 20)}) | CREATE (n:Node {{prop: {i}}}));", 1
        )
        execute_till_success(session, f"MATCH (s:Supernode), (n:Node {{prop: {i}}}) CREATE (s)<-[:REL]-(n);", 1)


def run_deleter() -> None:
    """
    Periodic deletion of an arbitrary subgraph in the graph
    """
    session = SessionCache.argument_session(args)
    try:
        print("Starting deleter")
        execute_till_success(session, "MATCH (n)-[r]->(m) DELETE r RETURN count(r)", 1)
        print("Deleter ended")
    except Exception as e:
        print("Deleter failed as it should with message: ")
        print(e)


def run_matcher() -> None:
    """
    Matching edges and returning the count.
    """
    session = SessionCache.argument_session(args)
    print("Starting matcher")
    execute_till_success(session, "MATCH (s:Supernode) RETURN COUNT(s)", 1)
    print("Matcher ended")


@timed_function("total_execution_time")
def execution_handler() -> None:
    clean_database()
    log.info("Database is clean.")

    setup_database_mode()
    create_indices()
    create_data()

    run_deleter()
    run_matcher()


if __name__ == "__main__":
    logging.basicConfig(level=args.logging)
    execution_handler()
    if args.logging in ["DEBUG", "INFO"]:
        output_data.dump()
