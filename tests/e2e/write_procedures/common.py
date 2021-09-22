import mgclient
import typing


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str,
                          params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def has_n_result_row(cursor: mgclient.Cursor, query: str, n: int):
    results = execute_and_fetch_all(cursor, query)
    return len(results) == n


def has_one_result_row(cursor: mgclient.Cursor, query: str):
    return has_n_result_row(cursor, query, 1)
