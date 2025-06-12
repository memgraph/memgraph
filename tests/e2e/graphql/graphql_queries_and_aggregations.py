import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./queries_and_aggregations.js")


def test_return_all_user_nodes_from_their_id_and_name(query_server):
    query_server.send_query("mutation { setup }")

    # cleanup_user_and_posts(query_server, user_uuid)
    query_server.send_query("mutation { teardown }")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
