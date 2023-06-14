import typing

import mgclient


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def execute_and_fetch_results_dict(cursor, query) -> typing.Dict:
    cursor.execute(query)
    return cursor.fetchall()[0][0]
