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


import os  # To be removed
import sys

import pytest
from common import connect, create_multi_db, execute_and_fetch_all, switch_db

COMMON_PATH_PREFIX_TEST1 = "procedures/mage/test_module"
COMMON_PATH_PREFIX_TEST2 = "procedures/new_test_module_utils"

FUNC1_PATH = os.path.join(
    os.path.dirname(__file__),
    COMMON_PATH_PREFIX_TEST1,
    "test_functions.py",
)

FUNC2_PATH = os.path.join(
    os.path.dirname(__file__),
    COMMON_PATH_PREFIX_TEST1,
    "test_functions_dir/test_subfunctions.py",
)

FUNC3_PATH = os.path.join(
    os.path.dirname(__file__),
    COMMON_PATH_PREFIX_TEST2,
    "new_test_functions.py",
)

FUNC4_PATH = os.path.join(
    os.path.dirname(__file__),
    COMMON_PATH_PREFIX_TEST2,
    "new_test_functions_dir/new_test_subfunctions.py",
)


def preprocess_functions(path1: str, path2: str):
    with open(path1, "w") as func1_file:
        func1_file.write(
            """def test_function(a: int, b: int) -> int:
    return a - b
            """
        )

    with open(path2, "w") as func2_file:
        func2_file.write(
            """def test_subfunction(a: int, b: int) -> int:
    return a / b
            """
        )


def postprocess_functions(path1: str, path2: str):
    with open(path1, "w") as func1_file:
        func1_file.write(
            """def test_function(a: int, b: int) -> int:
    return a + b
            """
        )

    with open(path2, "w") as func2_file:
        func2_file.write(
            """def test_subfunction(a: int, b: int) -> int:
    return a * b
            """
        )


@pytest.mark.parametrize("switch", [False, True])
def test_mg_load_reload_submodule_root_utils(switch):
    """Tests whether mg.load reloads content of some submodule code."""
    cursor = connect().cursor()
    if switch:
        create_multi_db(cursor)
        switch_db(cursor)
    # First do a simple experiment
    test_module_res = execute_and_fetch_all(cursor, "CALL new_test_module.test(10, 2) YIELD * RETURN *;")
    try:
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Now modify content of test function
        preprocess_functions(FUNC3_PATH, FUNC4_PATH)
        # Test that it doesn't work without calling reload
        test_module_res = execute_and_fetch_all(cursor, "CALL new_test_module.test(10, 2) YIELD * RETURN *;")
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Reload module
        execute_and_fetch_all(cursor, "CALL mg.load('new_test_module');")
        test_module_res = execute_and_fetch_all(cursor, "CALL new_test_module.test(10, 2) YIELD * RETURN *;")
        assert test_module_res[0][0] == 8  # - operator
        assert test_module_res[0][1] == 5  # / operator
    finally:
        # Revert to the original state for the consistency
        postprocess_functions(FUNC3_PATH, FUNC4_PATH)
    execute_and_fetch_all(cursor, "CALL mg.load('new_test_module');")


@pytest.mark.parametrize("switch", [False, True])
def test_mg_load_all_reload_submodule_root_utils(switch):
    """Tests whether mg.load_all reloads content of some submodule code"""
    cursor = connect().cursor()
    if switch:
        create_multi_db(cursor)
        switch_db(cursor)
    # First do a simple experiment
    test_module_res = execute_and_fetch_all(cursor, "CALL new_test_module.test(10, 2) YIELD * RETURN *;")
    try:
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Now modify content of test function
        preprocess_functions(FUNC3_PATH, FUNC4_PATH)
        # Test that it doesn't work without calling reload
        test_module_res = execute_and_fetch_all(cursor, "CALL new_test_module.test(10, 2) YIELD * RETURN *;")
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Reload module
        execute_and_fetch_all(cursor, "CALL mg.load_all();")
        test_module_res = execute_and_fetch_all(cursor, "CALL new_test_module.test(10, 2) YIELD * RETURN *;")
        assert test_module_res[0][0] == 8  # - operator
        assert test_module_res[0][1] == 5  # / operator
    finally:
        # Revert to the original state for the consistency
        postprocess_functions(FUNC3_PATH, FUNC4_PATH)
    execute_and_fetch_all(cursor, "CALL mg.load_all();")


@pytest.mark.parametrize("switch", [False, True])
def test_mg_load_reload_submodule(switch):
    """Tests whether mg.load reloads content of some submodule code."""
    cursor = connect().cursor()
    if switch:
        create_multi_db(cursor)
        switch_db(cursor)
    # First do a simple experiment
    test_module_res = execute_and_fetch_all(cursor, "CALL test_module.test(10, 2) YIELD * RETURN *;")
    try:
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Now modify content of test function
        preprocess_functions(FUNC1_PATH, FUNC2_PATH)
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
        postprocess_functions(FUNC1_PATH, FUNC2_PATH)
    execute_and_fetch_all(cursor, "CALL mg.load('test_module');")


@pytest.mark.parametrize("switch", [False, True])
def test_mg_load_all_reload_submodule(switch):
    """Tests whether mg.load_all reloads content of some submodule code"""
    cursor = connect().cursor()
    if switch:
        create_multi_db(cursor)
        switch_db(cursor)
    # First do a simple experiment
    test_module_res = execute_and_fetch_all(cursor, "CALL test_module.test(10, 2) YIELD * RETURN *;")
    try:
        assert test_module_res[0][0] == 12  # + operator
        assert test_module_res[0][1] == 20  # * operator
        # Now modify content of test function
        preprocess_functions(FUNC1_PATH, FUNC2_PATH)
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
        postprocess_functions(FUNC1_PATH, FUNC2_PATH)
    execute_and_fetch_all(cursor, "CALL mg.load_all();")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
