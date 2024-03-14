import multiprocessing

import mgclient
import pytest


def inner(query, number_of_executions):
    connection = mgclient.connect(host="127.0.0.1", port=7687)
    connection.autocommit = False
    cursor = connection.cursor()
    for _ in range(number_of_executions):
        cursor.execute(query)
    cursor.fetchall()


class MemgraphClient:
    def __init__(self):
        self.query_list = []

    def initialize_to_execute(self, query: str, number_of_executions):
        self.query_list.append((query, number_of_executions))

    def execute_queries(self):
        num_processes = len(self.query_list)
        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(inner, self.query_list)

        return True


@pytest.fixture
def client() -> MemgraphClient:
    return MemgraphClient()
