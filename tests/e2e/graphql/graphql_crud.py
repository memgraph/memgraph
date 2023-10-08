import sys

import pytest
from graphql_server import *


def test_create_query(query_server):
    query = 'mutation{createUsers(input:[{name:"John Doe"}]){users{id name}}}'
    gotten = query_server.send_query(query)
    expected_result = (
        '{"data":{"createUsers":{"users":[{"id":"e2d65187-d522-47bf-9791-6c66dd8fd672","name":"John Doe"}]}}}'
    )
    assert server_returned_expected(expected_result, gotten)


def test_nested_create_query(query_server):
    query = """
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

    expected_result = '{"data":{"createUsers":{"users":[{"id": "361004b7-f92d-4df0-9f96-5b43602c0f25","name": "John Doe","posts":[{"id":"e8d2033f-c15e-4529-a4f8-ca2ae09a066b",       "content": "Hi, my name is John!"}]}]}}}'
    gotten_response = query_server.send_query(query)
    assert server_returned_expected(expected_result, gotten_response)


def test_delete_node_query(query_server):
    created_node_uuid = create_node_query(query_server)

    delete_query = 'mutation{deleteUsers(where:{id:"' + created_node_uuid + '"}){nodesDeleted relationshipsDeleted}}'
    expected_delete_response = '{"data":{"deleteUsers":{"nodesDeleted":1,"relationshipsDeleted":0}}}\n'

    gotten = query_server.send_query(delete_query)
    assert expected_delete_response == str(gotten.text)


def test_nested_delete_node_query(query_server):
    node_uuids = create_related_nodes_query(query_server)
    created_user_uuid = node_uuids[0]

    delete_query = (
        'mutation {deleteUsers(where: {id: "'
        + created_user_uuid
        + '"},delete: {posts: {where: {}}}) {nodesDeleted relationshipsDeleted}}'
    )
    expected_delete_response = '{"data":{"deleteUsers":{"nodesDeleted":2,"relationshipsDeleted":1}}}\n'

    gotten = query_server.send_query(delete_query)
    assert expected_delete_response == str(gotten.text)


def test_update_node(query_server):
    node_uuids = create_related_nodes_query(query_server)
    created_post_uuid = node_uuids[1]

    update_query = (
        'mutation {updatePosts(where: {id: "'
        + created_post_uuid
        + '"}update: {content: "Some new content for this Post!"}) {posts {content}}}'
    )
    expected_update_response = '{"data":{"updatePosts":{"posts":[{"content":"Some new content for this Post!"}]}}}\n'

    gotten = query_server.send_query(update_query)
    assert expected_update_response == str(gotten.text)


def test_connect_or_create(query_server):
    created_user_uuid = create_node_query(query_server)

    connect_or_create_query = (
        'mutation {updateUsers(update: {posts: {connectOrCreate: {where: { node: { id: "1234" } }onCreate: { node: { content: "Some content" } }}}},where: { id: "'
        + created_user_uuid
        + '" }) {info {nodesCreated}}}'
    )
    expected_response = '{"data":{"updateUsers":{"info":{"nodesCreated":1}}}}\n'

    gotten = query_server.send_query(connect_or_create_query)
    assert expected_response == str(gotten.text)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
