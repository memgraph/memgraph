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
Large scale stress test. Tests only node creation.

The idea is to run this test on machines with huge amount of memory e.g. 2TB.
"""

import logging
import multiprocessing
import random
import time
from collections import defaultdict

from common import argument_session, connection_argument_parser


def parse_args():
    """
    Parses user arguments

    :return: parsed arguments
    """
    parser = connection_argument_parser()

    # specific
    parser.add_argument(
        "--worker-count", type=int, default=multiprocessing.cpu_count(), help="Number of concurrent workers."
    )
    parser.add_argument(
        "--logging", default="INFO", choices=["INFO", "DEBUG", "WARNING", "ERROR"], help="Logging level"
    )
    parser.add_argument("--vertex-count", type=int, default=100, help="Number of created vertices.")
    parser.add_argument(
        "--max-property-value",
        type=int,
        default=1000,
        help="Maximum value of property - 1. A created node "
        "will have a property with random value from 0 to "
        "max_property_value - 1.",
    )
    parser.add_argument("--create-pack-size", type=int, default=1, help="Number of CREATE clauses in a query")
    parser.add_argument("--isolation-level", type=str, required=True, help="Database isolation level.")
    parser.add_argument("--storage-mode", type=str, required=True, help="Database storage mode.")
    return parser.parse_args()


log = logging.getLogger(__name__)
args = parse_args()


def create_worker(worker_id):
    """
    Creates nodes and checks that all nodes were created.

    :param worker_id: worker id

    :return: tuple (worker_id, create execution time, time unit)
    """
    assert args.vertex_count > 0, "Number of vertices has to be positive int"

    generated_xs = defaultdict(int)
    create_query = ""
    with argument_session(args) as session:
        # create vertices
        start_time = time.time()
        for i in range(0, args.vertex_count):
            random_number = random.randint(0, args.max_property_value - 1)
            generated_xs[random_number] += 1
            create_query += "CREATE (:Label_T%s {x: %s}) " % (worker_id, random_number)
            # if full back or last item -> execute query
            if (i + 1) % args.create_pack_size == 0 or i == args.vertex_count - 1:
                session.run(create_query).consume()
                create_query = ""
        create_time = time.time()
        # check total count
        result_set = session.run("MATCH (n:Label_T%s) RETURN count(n) AS cnt" % worker_id).data()[0]
        assert result_set["cnt"] == args.vertex_count, "Create vertices Expected: %s Created: %s" % (
            args.vertex_count,
            result_set["cnt"],
        )
        # check count per property value
        for i, size in generated_xs.items():
            result_set = session.run("MATCH (n:Label_T%s {x: %s}) " "RETURN count(n) AS cnt" % (worker_id, i)).data()[0]
            assert result_set["cnt"] == size, "Per x count isn't good " "(Label: Label_T%s, prop x: %s" % (worker_id, i)
        return (worker_id, create_time - start_time, "s")


def create_handler():
    """
    Initializes processes and starts the execution.
    """
    # instance cleanup
    with argument_session(args) as session:
        session.run("MATCH (n) DETACH DELETE n").consume()

        # create indices
        for i in range(args.worker_count):
            session.run("CREATE INDEX ON :Label_T" + str(i)).consume()

        # concurrent create execution & tests
        with multiprocessing.Pool(args.worker_count) as p:
            for worker_id, create_time, time_unit in p.map(create_worker, [i for i in range(args.worker_count)]):
                log.info("Worker ID: %s; Create time: %s%s" % (worker_id, create_time, time_unit))

        # check total count
        expected_total_count = args.worker_count * args.vertex_count
        total_count = session.run("MATCH (n) RETURN count(n) AS cnt").data()[0]["cnt"]
        assert total_count == expected_total_count, "Total vertex number: %s Expected: %s" % (
            total_count,
            expected_total_count,
        )


if __name__ == "__main__":
    logging.basicConfig(level=args.logging)
    if args.logging != "DEBUG":
        logging.getLogger("neo4j").setLevel(logging.WARNING)
    create_handler()
