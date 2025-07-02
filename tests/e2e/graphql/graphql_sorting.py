import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./sorting.js")


def test_sorting(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            movies(sort: [{ runtime: ASC }]) {
                title
                runtime
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data": {
                "movies": [
                {
                    "title": "Spirited Away",
                    "runtime": 125
                },
                {
                    "title": "Parasite",
                    "runtime": 132
                },
                {
                    "title": "The Matrix",
                    "runtime": 136
                },
                {
                    "title": "Inception",
                    "runtime": 148
                },
                {
                    "title": "Interstellar",
                    "runtime": 169
                }
                ]
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_sorting_in_relationship(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
        movies {
            title
            runtime
            actors(sort: [{ surname: ASC }]) {
            surname
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
                "movies": [
                    {
                        "title": "Inception",
                        "runtime": 148,
                        "actors": [
                            {
                                "surname": "DiCaprio"
                            },
                            {
                                "surname": "Gordon-Levitt"
                            },
                            {
                                "surname": "Page"
                            }
                        ]
                    },
                    {
                        "title": "The Matrix",
                        "runtime": 136,
                        "actors": [
                            {
                                "surname": "Fishburne"
                            },
                            {
                                "surname": "Moss"
                            },
                            {
                                "surname": "Reeves"
                            }
                        ]
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
