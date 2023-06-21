import sys

import pytest
from graphql_server import *


def test_create_query(create_query_server):
    create_query = """
        mutation {
        createUsers(input: [
            {
                name: "John Doe"
            }
        ])
            {
                users {
                    id
                    name
                }
            }
        }
    """

    create_expected_result = (
        '{"data":{"createUsers":{"users":[{"id":"e2d65187-d522-47bf-9791-6c66dd8fd672","name":"John Doe"}]}}}'
    )
    create_response = create_query_server.send_query(create_query)

    assert server_returned_expceted(create_expected_result, create_response)


# TODO
# 1. Find a good way, how to group graphql tests that can be executed together.

if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
