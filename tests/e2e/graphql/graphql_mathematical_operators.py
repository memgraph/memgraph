import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./mathematical_operators.js")


def test_increment_int(query_server):
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
