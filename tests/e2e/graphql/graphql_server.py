import collections.abc
import json
import os.path
import subprocess
import time
from uuid import UUID

import pytest

import requests


class GraphQLServer:
    def __init__(self, config_file_path: str):
        self.url = "http://127.0.0.1:4000"

        graphql_lib = subprocess.Popen(["node", config_file_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.__wait_process_to_init(graphql_lib.pid)

    def send_query(self, query: str, timeout=5.0) -> requests.Response:
        try:
            response = requests.post(self.url, json={"query": query}, timeout=timeout)
        except requests.exceptions.Timeout as err:
            print("Request to GraphQL server has timed out. Details:", err)
        else:
            return response

    def __wait_process_to_init(self, pid: int):
        path = "/proc/" + str(pid)
        while True:
            if os.path.exists(path):
                return
            time.sleep(1 / 100)


def _ordered(obj: any) -> any:
    if isinstance(obj, dict):
        return sorted((k, _ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(_ordered(x) for x in obj)
    else:
        return obj


def _flatten(x: any) -> list:
    result = []
    for el in x:
        if isinstance(x, collections.abc.Iterable) and not isinstance(el, str):
            result.extend(_flatten(el))
        else:
            result.append(el)
    return result


def _valid_uuid(uuid_to_test: any, version: int = 4) -> any:
    try:
        uuid_obj = UUID(uuid_to_test, version=version)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


def server_returned_expected(expected_string: str, server_response: requests.Response) -> bool:
    expected_json = json.loads(expected_string)
    server_response_json = json.loads(server_response.text)

    expected = _flatten(_ordered(expected_json))
    actual = _flatten(_ordered(server_response_json))

    for expected_item, actual_item in zip(expected, actual):
        if expected_item != actual_item:
            if not (_valid_uuid(expected_item)):
                return False

    return True


def get_uuid_from_response(response: requests.Response) -> list:
    response_json = json.loads(response.text)
    flattened_response = _flatten(_ordered(response_json))
    uuids = []
    for item in flattened_response:
        if _valid_uuid(item):
            uuids.append(str(item))
    return uuids


def create_node_query(server: GraphQLServer):
    query = 'mutation{createUsers(input:[{name:"John Doe"}]){users{id name}}}'
    expected_result = (
        '{"data":{"createUsers":{"users":[{"id":"e2d65187-d522-47bf-9791-6c66dd8fd672","name":"John Doe"}]}}}'
    )
    gotten = server.send_query(query), expected_result
    gotten_response = gotten[0]
    assert server_returned_expected(expected_result, gotten_response)
    uuids = get_uuid_from_response(gotten_response)
    assert len(uuids) == 1
    return uuids[0]


def create_related_nodes_query(server: GraphQLServer):
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

    gotten_response = server.send_query(query)
    assert server_returned_expected(expected_result, gotten_response)
    return get_uuid_from_response(gotten_response)


@pytest.fixture
def query_server() -> GraphQLServer:
    path = "/home/gvolfing/workspace/neo4j_graphql_example/create.js"
    print(path)
    return GraphQLServer(path)
