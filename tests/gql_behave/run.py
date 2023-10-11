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

import argparse
import os
import sys

import mgclient
from behave import configuration
from behave.__main__ import main as behave_main

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def add_config(option, **kwargs):
    found = False
    for config in configuration.options:
        try:
            config[0].index(option)
            found = True
        except ValueError:
            pass
    if found:
        return
    configuration.options.append(((option,), kwargs))


def main():
    argp = argparse.ArgumentParser()

    args_bool, args_value = [], []

    def add_argument(option, **kwargs):
        argp.add_argument(option, **kwargs)
        add_config(option, **kwargs)
        if "action" in kwargs and kwargs["action"].startswith("store"):
            val = False if kwargs["action"] == "store_true" else True
            args_bool.append((option, val))
        else:
            args_value.append(option)

    # Custom argument for test suite
    argp.add_argument("test_suite", help="test suite that should be executed")
    add_config("--test-suite")
    add_config("--test-directory")

    # Arguments that should be passed on to Behave
    add_argument("--db-host", default="127.0.0.1", help="server host (default is 127.0.0.1)")
    add_argument("--db-port", default="7687", help="server port (default is 7687)")
    add_argument("--db-user", default="memgraph", help="server user (default is memgraph)")
    add_argument("--db-pass", default="memgraph", help="server pass (default is memgraph)")
    add_argument("--stop", action="store_true", help="stop testing after first fail")
    add_argument("--single-fail", action="store_true", help="pause after failed scenario")
    add_argument("--single-scenario", action="store_true", help="pause after every scenario")
    add_argument("--single-feature", action="store_true", help="pause after every feature")
    add_argument("--stats-file", default="", help="statistics output file")
    add_argument("--storage-mode", help="Memgraph storage mode")

    # Parse arguments
    parsed_args = argp.parse_args()

    if parsed_args.storage_mode is None:
        if parsed_args.test_suite == "memgraph_V1_on_disk":
            parsed_args.storage_mode = "ON_DISK_TRANSACTIONAL"
        else:
            parsed_args.storage_mode = "IN_MEMORY_TRANSACTIONAL"

    print(f"Test suite: {parsed_args.test_suite}")
    print(f"Storage mode: {parsed_args.storage_mode}")

    if parsed_args.test_suite == "memgraph_V1_on_disk" and parsed_args.storage_mode != "ON_DISK_TRANSACTIONAL":
        raise Exception(
            "memgraph_V1_on_disk test suite can only be run with ON_DISK_TRANSACTIONAL storage mode. For other storage modes, use memgraph_V1 test suite."
        )

    if parsed_args.test_suite == "memgraph_V1" and parsed_args.storage_mode == "ON_DISK_TRANSACTIONAL":
        raise Exception(
            "memgraph_V1 test suite cannot be run with ON_DISK_TRANSACTIONAL storage mode. For ON_DISK_TRANSACTIONAL storage mode, use memgraph_V1_on_disk test suite."
        )

    # Find tests
    test_directory = os.path.join(SCRIPT_DIR, "tests", parsed_args.test_suite)

    # Create arguments for Behave
    behave_args = [test_directory]
    for arg_name in args_value:
        var_name = arg_name[2:].replace("-", "_")
        behave_args.extend([arg_name, getattr(parsed_args, var_name)])
    for arg_name, arg_val in args_bool:
        var_name = arg_name[2:].replace("-", "_")
        current = getattr(parsed_args, var_name)
        if current != arg_val:
            behave_args.append(arg_name)
    behave_args.extend(["--test-suite", parsed_args.test_suite])
    behave_args.extend(["--test-directory", test_directory])

    # Run Behave tests
    return behave_main(behave_args)


if __name__ == "__main__":
    sys.exit(main())
