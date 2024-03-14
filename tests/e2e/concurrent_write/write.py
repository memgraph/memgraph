import sys
import threading
import time
import typing

import mgclient
import pytest


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


commit_success_lock = threading.Lock()
commit_fail_lock = threading.Lock()


def client_success():
    commit_fail_lock.acquire()
    time.sleep(0.1)
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = False

    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    execute_and_fetch_all(cursor, "CREATE (:N1), (:N2);")
    connection.commit()

    execute_and_fetch_all(cursor, "MATCH (n1:N1) DELETE n1;")
    commit_success_lock.acquire()
    commit_fail_lock.release()
    connection.commit()
    commit_success_lock.release()


def client_fail():
    try:
        commit_success_lock.acquire()
        connection = mgclient.connect(host="localhost", port=7687)
        connection.autocommit = False
        cursor = connection.cursor()

        execute_and_fetch_all(cursor, "MATCH (n1:N1), (n2:N2) CREATE (n1)-[:R]->(n2);")
        commit_success_lock.release()
        commit_fail_lock.acquire()
        connection.commit()
    except mgclient.DatabaseError:
        commit_fail_lock.release()


def test_concurrent_write():
    t1 = threading.Thread(target=client_success)
    t2 = threading.Thread(target=client_fail)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    assert execute_and_fetch_all(cursor, "MATCH (n:N1) RETURN inDegree(n);") == []
    assert execute_and_fetch_all(cursor, "MATCH (n:N1) RETURN outDegree(n);") == []
    assert execute_and_fetch_all(cursor, "MATCH (n:N2) RETURN inDegree(n);")[0][0] == 0
    assert execute_and_fetch_all(cursor, "MATCH (n:N2) RETURN outDegree(n);")[0][0] == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
