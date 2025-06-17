import sys
from json import loads
from textwrap import dedent

import pytest
from graphql_server import *


def extract_cursor_from_response(response):
    return loads(response)["data"]["users"][0]["postsConnection"]["pageInfo"]["endCursor"]


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./cursor_based_pagination.js")


def test_cursor_based_pagination(query_server):
    query_server.send_query("mutation { setup }")

    query = dedent(
        """\
        query Users($after: String) {
          users(where: { name: { eq: "John Smith" } }) {
            postsConnection(after: $after, first: 5, sort: { node: { content: ASC} }) {
              pageInfo {
                endCursor
                hasNextPage
              }
              totalCount
              edges {
                node {
                    content
                }
              }
            }
          }
        }
        """
    ).strip()

    # First request, specify no cursor to start at the beginning.

    gotten = query_server.send_query(query, variables='{"after":""}')
    expected_result = dedent(
        """\
        {
          "data": {
            "users": [
              {
                "postsConnection": {
                  "pageInfo": {
                    "endCursor": "YXJyYXljb25uZWN0aW9uOjQ=",
                    "hasNextPage": true
                  },
                  "totalCount": 13,
                  "edges": [
                    {
                      "node": {
                        "content": "alfa"
                      }
                    },
                    {
                      "node": {
                        "content": "bravo"
                      }
                    },
                    {
                      "node": {
                        "content": "charlie"
                      }
                    },
                    {
                      "node": {
                        "content": "delta"
                      }
                    },
                    {
                      "node": {
                        "content": "echo"
                      }
                    }
                  ]
                }
              }
            ]
          }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    # Second request passes cursor from the end of previous results to retrieve
    # next full block of 5 results.

    cursor = extract_cursor_from_response(str(gotten.text))
    gotten = query_server.send_query(query, variables='{"after":"' + cursor + '"}')
    expected_result = dedent(
        """\
        {
          "data": {
            "users": [
              {
                "postsConnection": {
                  "pageInfo": {
                    "endCursor": "YXJyYXljb25uZWN0aW9uOjk=",
                    "hasNextPage": true
                  },
                  "totalCount": 13,
                  "edges": [
                    {
                      "node": {
                        "content": "foxtrot"
                      }
                    },
                    {
                      "node": {
                        "content": "gulf"
                      }
                    },
                    {
                      "node": {
                        "content": "hotel"
                      }
                    },
                    {
                      "node": {
                        "content": "india"
                      }
                    },
                    {
                      "node": {
                        "content": "juliett"
                      }
                    }
                  ]
                }
              }
            ]
          }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    # Third request passes cursor to retrieve final block of 3 results.

    cursor = extract_cursor_from_response(str(gotten.text))
    gotten = query_server.send_query(query, variables='{"after":"' + cursor + '"}')
    expected_result = dedent(
        """\
        {
          "data": {
            "users": [
              {
                "postsConnection": {
                  "pageInfo": {
                    "endCursor": "YXJyYXljb25uZWN0aW9uOjEy",
                    "hasNextPage": false
                  },
                  "totalCount": 13,
                  "edges": [
                    {
                      "node": {
                        "content": "kilo"
                      }
                    },
                    {
                      "node": {
                        "content": "lima"
                      }
                    },
                    {
                      "node": {
                        "content": "mike"
                      }
                    }
                  ]
                }
              }
            ]
          }
        }
        """
    ).strip()

    assert server_returned_expected(expected_result, gotten)

    # Requests using the cursor associated with the end of the results return no
    # more results.

    cursor = extract_cursor_from_response(str(gotten.text))
    gotten = query_server.send_query(query, variables='{"after":"' + cursor + '"}')
    expected_result = dedent(
        """\
        {
          "data": {
            "users": [
              {
                "postsConnection": {
                  "pageInfo": {
                    "endCursor": null,
                    "hasNextPage": false
                  },
                  "totalCount": 13,
                  "edges": []
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
