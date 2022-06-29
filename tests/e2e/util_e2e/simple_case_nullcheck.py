import typing
import mgclient
import sys
import pytest


def test_nullcheck_yields_error(connection):
    cursor = connection.cursor()
    query = """WITH 2 AS name
                RETURN CASE name
                WHEN 3 THEN 'works'
                WHEN null THEN "doesn't work"
                ELSE 'something went wrong'
                END"""

    cursor.execute(query)
    row = cursor.fetchone()

    assert row == "Use the generic form when checking against NULL."


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
