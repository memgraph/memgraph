import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./arrays.js")


def test_array_push(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        mutation {
            updateMovies (update: { tags: { push: "kung-fu" } }) {
                movies {
                    title
                    tags
                }
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data": {
                "updateMovies": {
                    "movies": [
                        {
                            "title": "The Matrix",
                            "tags": [
                                "action",
                                "sci-fi",
                                "kung-fu"
                            ]
                        }
                    ]
                }
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_array_pop(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        mutation {
            updateMovies (update: { tags: { pop: 1 } }) {
                movies {
                    title
                    tags
                }
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data": {
                "updateMovies": {
                    "movies": [
                        {
                            "title": "The Matrix",
                            "tags": [
                                "action"
                            ]
                        }
                    ]
                }
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
