import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./mathematical_operators.js")


def test_mutation_in__property(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        mutation {
            updateVideos(
                where: { id: { eq: "db3b98b6-0497-4f57-ae07-793eea62d1b3" } }
                update: { views: { add: 1 } }
            ) {
                videos {
                id
                views
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
                "updateVideos": {
                    "videos": [
                        {
                            "id": "db3b98b6-0497-4f57-ae07-793eea62d1b3",
                            "views": 43
                        }
                    ]
                }
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_mutation_in_relationship_property(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        mutation addRevenueMutation {
            updateUsers(
                where: { id: { eq: "fe2a1e27-b42f-4f11-93a8-690704afdb35" } }
                update: { ownVideo: [{ update: { edge: { revenue: { add: 7.5 } } } }] }
            ) {
                users {
                    id
                    ownVideoConnection {
                        edges {
                            properties {
                                revenue
                            }
                        }
                    }
                }
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    print(str(gotten.text))
    expected_result = dedent(
        """\
        {
            "data": {
                "updateUsers": {
                    "users": [
                        {
                            "id": "fe2a1e27-b42f-4f11-93a8-690704afdb35",
                            "ownVideoConnection": {
                                "edges": [
                                    {
                                        "properties": {
                                            "revenue": 8.5
                                        }
                                    }
                                ]
                            }
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
