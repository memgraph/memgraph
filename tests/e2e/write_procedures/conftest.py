import pytest

from common import execute_and_fetch_all, connect


@pytest.fixture(autouse=True)
def connection():
    connection = connect()
    yield connection
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
