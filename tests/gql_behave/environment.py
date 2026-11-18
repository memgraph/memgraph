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

import itertools
import json
import logging
import os
import sys

from neo4j import GraphDatabase, basic_auth
from steps import database
from steps.test_parameters import TestParameters

# Helper class and functions

# behave scopes attribute writes to the current layer, so a counter kept on the context would restart every scenario.
vbranch_counter = itertools.count(1)


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

    if getattr(context.config, "versioned_branch", False):
        if context.config.expected_failures:
            with open(context.config.expected_failures) as f:
                context.expected_failures = json.load(f)
        else:
            context.expected_failures = {}


def before_scenario(context, scenario):
    context.test_parameters = TestParameters()
    context.exception = None

    if getattr(context.config, "versioned_branch", False):
        # Defensive: a session reopened by cleanups (they run after after_scenario) dies with its scenario layer
        # and is never visible here.
        database.close_vbranch_session(context)
        context.vbranch_refused = False
        # Unique per scenario: a failed best-effort DROP BRANCH must not collide with the next CREATE BRANCH.
        context.vbranch_name = f"gqlb_{next(vbranch_counter)}"


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
    status = scenario.status
    versioned = getattr(context.config, "versioned_branch", False)
    if versioned:
        entry = context.expected_failures.get(os.path.basename(scenario.filename), {}).get(scenario.name)
        # A scenario that failed before the fork (setup) says nothing about the branch.
        failed_on_branch = getattr(context, "vbranch_forked", False) and (
            scenario.status == "failed" or context.vbranch_refused
        )
        if entry is not None:
            # error is the expected query error text; null means a wrong result with no query error. Anything else
            # (another error, a dropped connection) is a real regression.
            error = "" if context.exception is None else str(context.exception)
            matches = failed_on_branch and (entry["error"] in error if entry["error"] is not None else not error)
            status = "passed" if matches else "failed"
            if matches:
                print(f"\nExpected failure on a branch ({entry['reason']}): {scenario.name}")
            else:
                print(
                    f"\nListed in expected_failures but did not fail as expected on a branch "
                    f"(want error {entry['error']!r}, got {error!r}): {scenario.name}"
                )
        elif context.vbranch_refused:
            status = "failed"
            print(f"\nRefused on a branch but not listed in expected_failures: {scenario.name}")
    context.test_results.add_test(status)

    if versioned:
        database.close_vbranch_session(context)

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
