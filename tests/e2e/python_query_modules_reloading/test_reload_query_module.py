# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os  # To be removed
import sys

import pytest
from common import connect, execute_and_fetch_all

COMMON_PATH_PREFIX = "procedures/mage/test_module"
FUNC1_PATH = os.path.join(
    os.path.dirname(__file__),
    COMMON_PATH_PREFIX,
    "test_functions.py",
)

FUNC2_PATH = os.path.join(
    os.path.dirname(__file__),
    COMMON_PATH_PREFIX,
    "test_functions_dir/test_subfunctions.py",
)


def preprocess_functions():
    with open(FUNC1_PATH, "w") as func1_file:
        func1_file.write(
            """def test_function(a: int, b: int) -> int:
    return a - b
            """
        )

    with open(FUNC2_PATH, "w") as func2_file:
        func2_file.write(
            """def test_subfunction(a: int, b: int) -> int:
    return a / b
            """
        )


def postprocess_functions():
    with open(FUNC1_PATH, "w") as func1_file:
        func1_file.write(
            """def test_function(a: int, b: int) -> int:
    return a + b
            """
        )

    with open(FUNC2_PATH, "w") as func2_file:
        func2_file.write(
            """def test_subfunction(a: int, b: int) -> int:
    return a * b
            """
        )


def test_mg_load_reload_submodule():
    """Tests whether mg.load reloads content of some submodule code."""
    cursor = connect().cursor()
    # First do a simple experiment
    test_module_res = execute_and_fetch_all(cursor, "CALL test_module.test(10, 2) YIELD * RETURN *;")
    try:
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Now modify content of test function
        preprocess_functions()
        # Test that it doesn't work without calling reload
        test_module_res = execute_and_fetch_all(cursor, "CALL test_module.test(10, 2) YIELD * RETURN *;")
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Reload module
        execute_and_fetch_all(cursor, "CALL mg.load('test_module');")
        test_module_res = execute_and_fetch_all(cursor, "CALL test_module.test(10, 2) YIELD * RETURN *;")
        assert test_module_res[0][0] == 8  # - operator
        assert test_module_res[0][1] == 5  # / operator
    finally:
        # Revert to the original state for the consistency
        postprocess_functions()
    execute_and_fetch_all(cursor, "CALL mg.load('test_module');")


def test_mg_load_all_reload_submodule():
    """Tests whether mg.load_all reloads content of some submodule code"""
    cursor = connect().cursor()
    # First do a simple experiment
    test_module_res = execute_and_fetch_all(cursor, "CALL test_module.test(10, 2) YIELD * RETURN *;")
    try:
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Now modify content of test function
        preprocess_functions()
        # Test that it doesn't work without calling reload
        test_module_res = execute_and_fetch_all(cursor, "CALL test_module.test(10, 2) YIELD * RETURN *;")
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Reload module
        execute_and_fetch_all(cursor, "CALL mg.load_all();")
        test_module_res = execute_and_fetch_all(cursor, "CALL test_module.test(10, 2) YIELD * RETURN *;")
        assert test_module_res[0][0] == 8  # - operator
        assert test_module_res[0][1] == 5  # / operator
    finally:
        # Revert to the original state for the consistency
        postprocess_functions()
    execute_and_fetch_all(cursor, "CALL mg.load_all();")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
