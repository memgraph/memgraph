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
                                "content":"Third one"
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


def test_aggregation(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query Name {
            usersConnection {
                aggregate {
                node {
                    name {
                    longest
                    }
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
                "usersConnection": {
                    "aggregate": {
                        "node": {
                            "name": {
                                "longest": "Alice"
                            }
                        }
                    }
                }
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_aggregation_with_filter(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            postsConnection(where: { content: { contains: "post" } }) {
                aggregate {
                    count {
                        nodes
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
                "postsConnection": {
                    "aggregate": {
                        "count": {
                            "nodes": 2
                        }
                    }
                }
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_aggregation_over_related_nodes(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            users {
                id
                postsConnection {
                    aggregate {
                        count {
                            nodes
                        }
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
                "users": [
                    {
                        "id": "51f65ea1-b612-47e6-8cc1-c13735168130",
                        "postsConnection": {
                            "aggregate": {
                                "count": {
                                    "nodes": 3
                                }
                            }
                        }
                    },
                    {
                        "id": "02bae290-1943-49e6-8be8-f15c1a0c5923",
                        "postsConnection": {
                            "aggregate": {
                                "count": {
                                    "nodes": 1
                                }
                            }
                        }
                    }
                ]
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_aggregation_longest_post_per_user(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            users {
                name
                postsConnection {
                    aggregate {
                        node {
                            content {
                                longest
                            }
                        }
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
                "users": [
                {
                        "name": "Alice",
                        "postsConnection": {
                            "aggregate": {
                                "node": {
                                    "content": {
                                        "longest": "Second post"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "name": "Bob",
                        "postsConnection": {
                            "aggregate": {
                                "node": {
                                    "content": {
                                        "longest": "Fourth one"
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


def test_edge_date_aggregation(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query {
            users {
                name
                postsConnection {
                    aggregate {
                        edge {
                            date {
                                max
                            }
                        }
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
                "users": [
                    {
                        "name": "Alice",
                        "postsConnection": {
                            "aggregate": {
                                "edge": {
                                    "date": {
                                        "max": "2011-12-05T09:15:30.000Z"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "name": "Bob",
                        "postsConnection": {
                            "aggregate": {
                                "edge": {
                                    "date": {
                                        "max": "2011-12-06T09:15:30.000Z"
                                    }
                                }
                            }
                        }
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
