import sys
import threading
import time
import typing

import mgclient
import pytest


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


class AtomicInteger:
    def __init__(self, value=0):
        self._value = int(value)
        self._lock = threading.Lock()

    def inc(self, d=1):
        with self._lock:
            self._value += int(d)
            return self._value

    def dec(self, d=1):
        return self.inc(-d)

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = int(v)
            return self._value


def sleep_until(wanted_cnt):
    while cnt.value != wanted_cnt:
        time.sleep(0.1)


def client_success():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = False
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "MATCH (n1:N1) DELETE n1;")
    cnt.inc()  # 1
    sleep_until(2)

    connection.commit()
    cnt.inc()  # 3


def client_fail():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = False
    cursor = connection.cursor()

    try:
        sleep_until(1)
        execute_and_fetch_all(cursor, "MATCH (n1:N1), (n2:N2) CREATE (n1)-[:R]->(n2);")
        cnt.inc()  # 2

        sleep_until(3)
        connection.commit()  # this should fail
    except mgclient.DatabaseError:
        return
    except Exception:
        assert False


def test_concurrent_write():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    execute_and_fetch_all(cursor, "CREATE (:N1), (:N2);")

    t1 = threading.Thread(target=client_success)
    t2 = threading.Thread(target=client_fail)

    global cnt
    cnt = AtomicInteger(0)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    assert execute_and_fetch_all(cursor, "MATCH (n:N1) RETURN inDegree(n);") == []
    assert execute_and_fetch_all(cursor, "MATCH (n:N1) RETURN outDegree(n);") == []
    assert execute_and_fetch_all(cursor, "MATCH (n:N2) RETURN inDegree(n);")[0][0] == 0
    assert execute_and_fetch_all(cursor, "MATCH (n:N2) RETURN outDegree(n);")[0][0] == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
