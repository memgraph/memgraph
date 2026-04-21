import sys

import pytest
from common import connect, execute_and_fetch_all


def test_void_proc(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CALL void_procedures.dummy();")


def test_void_proc_yield_asterisk_fails(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL void_procedures.dummy() YIELD * RETURN *;")


def test_void_proc_yield_field_fails(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL void_procedures.dummy() YIELD n RETURN n;")


def test_signature_returns_but_impl_does_not(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(
            cursor, "CALL void_procedures.signature_returns_but_impl_does_not() YIELD result RETURN result;"
        )


def test_signature_void_but_impl_returns(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL void_procedures.signature_void_but_impl_returns();")


def test_signature_void_but_impl_returns_yield_fails(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL void_procedures.signature_void_but_impl_returns() YIELD * RETURN *;")


def test_explicit_empty_record(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CALL void_procedures.explicit_empty_record();")


def test_explicit_empty_record_yield_asterisk_fails(connection):
    cursor = connection.cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL void_procedures.explicit_empty_record() YIELD * RETURN *;")


def test_void_proc_called_multiple_times(connection):
    cursor = connection.cursor()
    result = execute_and_fetch_all(
        cursor, "UNWIND range(1, 5) AS i CALL void_procedures.dummy() RETURN count(i) AS cnt;"
    )
    assert result[0][0] == 5


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
