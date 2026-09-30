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

# -*- coding: utf-8 -*-

import json
import logging
import sys

from neo4j import GraphDatabase, basic_auth
from steps.test_parameters import TestParameters

# Helper class and functions


class TestResults:
    def __init__(self):
        self.total = 0
        self.passed = 0

    def num_passed(self):
        return self.passed

    def num_total(self):
        return self.total

    def add_test(self, status):
        if status == "passed":
            self.passed += 1
        self.total += 1


# Behave specific functions


def before_all(context):
    # logging
    logging.basicConfig(level="DEBUG")
    context.log = logging.getLogger(__name__)

    # driver
    uri = "bolt://{}:{}".format(context.config.db_host, context.config.db_port)
    auth_token = basic_auth(context.config.db_user, context.config.db_pass)
    context.driver = GraphDatabase.driver(uri, auth=auth_token, encrypted=False)

    # test results
    context.test_results = TestResults()


def before_scenario(context, scenario):
    context.test_parameters = TestParameters()
    context.exception = None


def after_step(context, step):
    """Print actual executed query on step failure when parallel execution modifies the query."""
    if step.status == "failed":
        parallel_execution = getattr(context.config, "parallel_execution", False)
        actual_query = getattr(context, "last_executed_query", None)

        if parallel_execution:
            print("\n" + "=" * 60)
            print("PARALLEL EXECUTION MODE ENABLED")
            if actual_query:
                print(f"Actual query executed:\n{actual_query}")
            print("=" * 60 + "\n")
        elif actual_query and actual_query != getattr(step, "text", ""):
            print(f"\nActual query executed:\n{actual_query}\n")


def after_scenario(context, scenario):
    context.test_results.add_test(scenario.status)
    if context.config.single_scenario or (context.config.single_fail and scenario.status == "failed"):
        print("Press enter to continue")
        sys.stdin.readline()


def after_feature(context, feature):
    if context.config.single_feature:
        print("Press enter to continue")
        sys.stdin.readline()


def after_all(context):
    context.driver.close()

    if context.config.stats_file == "":
        return

    js = {
        "total": context.test_results.num_total(),
        "passed": context.test_results.num_passed(),
        "test_suite": context.config.test_suite,
    }

    with open(context.config.stats_file, "w") as f:
        json.dump(js, f)
