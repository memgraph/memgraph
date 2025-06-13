import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./queries_and_aggregations.js")


def test_return_all_user_nodes_from_their_id_and_name(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            users {
                id
                name
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data":{
                "users":[
                    {
                        "id":"51f65ea1-b612-47e6-8cc1-c13735168130",
                        "name":"Alice"
                    },
                    {
                        "id":"02bae290-1943-49e6-8be8-f15c1a0c5923",
                        "name":"Bob"
                    }
                ]
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_query_user_and_return_all_their_posts(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            users(where: { name: { eq: "Alice" } }) {
                id
                name
                posts {
                    content
                }
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data":{
                "users":[
                    {
                        "name":"Alice",
                        "id":"51f65ea1-b612-47e6-8cc1-c13735168130",
                        "posts":[
                            {
                                "content":"First post"
                            },
                            {
                                "content":"Second post"
                            },
                            {
                                "content":"Third post"
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
