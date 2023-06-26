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


def ordered(obj: any) -> any:
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def flatten(x: any) -> list:
    result = []
    for el in x:
        if isinstance(x, collections.abc.Iterable) and not isinstance(el, str):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result


def valid_uuid(uuid_to_test: any, version: int = 4) -> any:
    try:
        uuid_obj = UUID(uuid_to_test, version=version)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


def server_returned_expceted(expected_string: str, server_response: requests.Response) -> bool:
    expected_json = json.loads(expected_string)
    server_response_json = json.loads(server_response.text)

    expected = flatten(ordered(expected_json))
    actual = flatten(ordered(server_response_json))

    for expected_item, actual_item in zip(expected, actual):
        if expected_item != actual_item:
            if not (valid_uuid(expected_item)):
                return False

    return True


@pytest.fixture
def create_query_server() -> GraphQLServer:
    path = "../../build/tests/e2e/graphql/graphql_library_config/create.js"
    return GraphQLServer(path)
