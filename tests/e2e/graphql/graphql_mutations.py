import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./mutations.js")


def test_create(query_server):
    query = dedent(
        """\
        mutation {
            createUsers(input: [
                {
                    name: "John Doe"
                }
            ]) {
                users {
                    id
                    name
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
                "createUsers": {
                    "users": [
                        {
                            "id": "bd4b94ac-c405-412a-b040-0c8091138192",
                            "name": "John Doe"
                        }
                    ]
                }
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_nested_create(query_server):
    query = dedent(
        """\
        mutation {
            createUsers(input: [
                {
                    name: "John Doe"
                    posts: {
                        create: [
                            {
                                node: {
                                    content: "Hi, my name is John!"
                                }
                            }
                        ]
                    }
                }
            ]) {
                users {
                    id
                    name
                    posts {
                        id
                        content
                    }
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
                "createUsers": {
                    "users": [
                        {
                            "id": "6c8c15ec-7d50-4e52-9563-04cf3d1751be",
                            "name": "John Doe",
                            "posts": [
                                {
                                    "id": "4c80fe26-d503-429d-a9ee-2a6aa6e96891",
                                    "content": "Hi, my name is John!"
                                }
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
