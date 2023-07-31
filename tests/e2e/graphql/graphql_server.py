import atexit
import collections.abc
import json
import os.path
import socket
import subprocess
import time
from uuid import UUID

import pytest

import requests


class GraphQLServer:
    def __init__(self, config_file_path: str):
        self.url = "http://127.0.0.1:4000"

        self.graphql_lib = subprocess.Popen(["node", config_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.__wait_process_to_init(7687)
        self.__wait_process_to_init(4000)
        atexit.register(self.__shut_down)

    def send_query(self, query: str, timeout=5.0) -> requests.Response:
        try:
            response = requests.post(self.url, json={"query": query}, timeout=timeout)
        except requests.exceptions.Timeout as err:
            print("Request to GraphQL server has timed out. Details:", err)
        else:
            return response

    def __wait_process_to_init(self, port):
        host = "127.0.0.1"
        try:
            while True:
                # Create a socket object
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    result = s.connect_ex((host, port))
                    if result == 0:
                        break

        except socket.error as e:
            print(f"Error occurred while checking port {port}: {e}")
            return False

    def __shut_down(self):
        self.graphql_lib.kill()
        ls = subprocess.Popen(("lsof", "-t", "-i:4000"), stdout=subprocess.PIPE)
        subprocess.check_output(("xargs", "-r", "kill"), stdin=ls.stdout)
        ls.wait()


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
        if expected_item != actual_item and not (_valid_uuid(expected_item)):
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
    gotten = server.send_query(query)
    uuids = get_uuid_from_response(gotten)
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

    gotten_response = server.send_query(query)
    return get_uuid_from_response(gotten_response)


@pytest.fixture
def query_server() -> GraphQLServer:
    path = os.path.join("graphql/graphql_library_config/crud.js")
    return GraphQLServer(path)
