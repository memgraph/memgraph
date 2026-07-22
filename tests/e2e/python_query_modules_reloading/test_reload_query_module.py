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

import mgclient
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

RELOAD_FUNC_MODULE_PATH = os.path.join(os.path.dirname(__file__), "procedures", "reload_func_module.py")


def write_combine_function(op: str):
    with open(RELOAD_FUNC_MODULE_PATH, "w") as func_file:
        func_file.write(
            f"""import mgp


@mgp.function
def combine(ctx: mgp.FuncCtx, a: mgp.Number, b: mgp.Number):
    return a {op} b
"""
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


@pytest.mark.parametrize("switch", [False, True])
def test_mg_load_reload_magic_function(switch):
    cursor = connect().cursor()
    if switch:
        create_multi_db(cursor)
        switch_db(cursor)
    write_combine_function("+")
    execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")
    try:
        assert execute_and_fetch_all(cursor, "RETURN reload_func_module.combine(2, 3) AS r;")[0][0] == 5
        assert execute_and_fetch_all(cursor, "RETURN reload_func_module.combine(2, 3) AS r;")[0][0] == 5
        write_combine_function("*")
        execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")
        assert execute_and_fetch_all(cursor, "RETURN reload_func_module.combine(2, 3) AS r;")[0][0] == 6
    finally:
        write_combine_function("+")
        execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")


def test_mg_load_removing_used_magic_function_errors():
    cursor = connect().cursor()
    write_combine_function("+")
    execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")
    try:
        assert execute_and_fetch_all(cursor, "RETURN reload_func_module.combine(2, 3) AS r;")[0][0] == 5
        with open(RELOAD_FUNC_MODULE_PATH, "w") as func_file:
            func_file.write("import mgp\n")
        execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(cursor, "RETURN reload_func_module.combine(2, 3) AS r;")
    finally:
        write_combine_function("+")
        execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")


def test_multiple_magic_functions_in_one_query():
    cursor = connect().cursor()
    with open(RELOAD_FUNC_MODULE_PATH, "w") as func_file:
        func_file.write(
            "import mgp\n\n\n"
            "@mgp.function\n"
            "def combine(ctx: mgp.FuncCtx, a: mgp.Number, b: mgp.Number):\n"
            "    return a + b\n\n\n"
            "@mgp.function\n"
            "def diff(ctx: mgp.FuncCtx, a: mgp.Number, b: mgp.Number):\n"
            "    return a - b\n"
        )
    execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")
    try:
        row = execute_and_fetch_all(
            cursor, "RETURN reload_func_module.combine(5, 3) AS c, reload_func_module.diff(5, 3) AS d;"
        )[0]
        assert row[0] == 8
        assert row[1] == 2
    finally:
        write_combine_function("+")
        execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")


@pytest.mark.parametrize("switch", [False, True])
def test_trigger_using_magic_function_reload(switch):
    cursor = connect().cursor()
    if switch:
        create_multi_db(cursor)
        switch_db(cursor)
    write_combine_function("+")
    execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")
    try:
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(
            cursor,
            "CREATE TRIGGER combine_trigger ON () CREATE BEFORE COMMIT "
            "EXECUTE UNWIND createdVertices AS v SET v.r = reload_func_module.combine(2, 3);",
        )
        execute_and_fetch_all(cursor, "CREATE (:A);")
        assert execute_and_fetch_all(cursor, "MATCH (n:A) RETURN n.r;")[0][0] == 5
        write_combine_function("*")
        execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")
        execute_and_fetch_all(cursor, "CREATE (:B);")
        assert execute_and_fetch_all(cursor, "MATCH (n:B) RETURN n.r;")[0][0] == 6
    finally:
        try:
            execute_and_fetch_all(cursor, "DROP TRIGGER combine_trigger;")
        except mgclient.DatabaseError:
            pass
        write_combine_function("+")
        execute_and_fetch_all(cursor, "CALL mg.load('reload_func_module');")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
