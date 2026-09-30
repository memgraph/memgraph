import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./pagination.js")


def test_pagination_limit(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            users(limit: 5) {
                name
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data": {
                "users": [
                    {
                        "name": "Alice"
                    },
                    {
                        "name": "Bob"
                    },
                    {
                        "name": "Charlie"
                    },
                    {
                        "name": "Diana"
                    },
                    {
                        "name": "Eve"
                    }
                ]
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_pagination_offset(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            users(limit: 5, offset: 7) {
                name
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data": {
                "users": [
                    {
                        "name": "Hank"
                    },
                    {
                        "name": "Ivy"
                    },
                    {
                        "name": "Jack"
                    },
                    {
                        "name": "Kara"
                    },
                    {
                        "name": "Leo"
                    }
                ]
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
