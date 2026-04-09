import sys

import pytest
from common import connect, execute_and_fetch_all


def test_void_proc(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CALL void_procs.void_proc();")


def test_void_proc_yield_asterisk_fails(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL void_procs.void_proc() YIELD * RETURN *;")


def test_void_proc_yield_field_fails(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL void_procs.void_proc() YIELD n RETURN n;")


def test_signature_returns_but_impl_does_not(connection):
    cursor = connection.cursor()
    result = execute_and_fetch_all(
        cursor, "CALL void_procs.signature_returns_but_impl_does_not() YIELD result RETURN result;"
    )
    assert len(result) == 0


def test_signature_void_but_impl_returns(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CALL void_procs.signature_void_but_impl_returns();")


def test_signature_void_but_impl_returns_yield_fails(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL void_procs.signature_void_but_impl_returns() YIELD * RETURN *;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
