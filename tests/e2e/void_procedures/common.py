import mgclient


def connect(**kwargs):
    connection = mgclient.connect(host="127.0.0.1", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def execute_and_fetch_all(cursor, query, params={}):
    cursor.execute(query, params)
    return cursor.fetchall()
