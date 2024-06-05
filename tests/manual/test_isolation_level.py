#!/usr/bin/env python3

"""
This script verifies whether the server is able to correctly
detect and abort certain classes of transactional anomaly,
inspired by the hermitage tests found at:
    https://github.com/ept/hermitage
and based on the correctness definitions laid out in:
    https://pmg.csail.mit.edu/papers/adya-phd.pdf

As of the time of committing this script, the database
is able to meet the conditions required for Snapshot Isolation
and Consistent View.

All of these tests begin with a dataset of:
    Kv { key: 1, value: 10 }
    Kv { key: 2, value: 20 }

and then assert various transactional correctness properties.

Update: 06/2024
Tests are written in a form: Does isolation level with which Memgraph DB is started exhibit certain phenomenon?
"""

import argparse
import typing

import mgclient


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def setup(conn):
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("CREATE CONSTRAINT ON (kv: Kv) ASSERT kv.key IS UNIQUE;")
    conn.commit()
    conn.autocommit = False

    cursor = conn.cursor()
    cursor.execute("MATCH (kv) DETACH DELETE kv;")
    conn.commit()

    update(cursor, 1, 10, "instantiate key 1")
    conn.commit()
    update(cursor, 2, 20, "instantiate key 2")
    conn.commit()

    assert_eq(get(cursor, 1), [10])
    assert_eq(get(cursor, 2), [20])


def update(cursor, key, value, comment=""):
    cursor.execute(
        """
            // %s
         MERGE (kv:Kv{ key: $key })
           SET kv.value = $value
        RETURN kv;
    """
        % comment,
        {"key": key, "value": value},
    )
    ret = cursor.fetchall()
    if len(ret) != 1:
        print(f"expected ret to be len 1, but it's {ret}")
        assert len(ret) == 1
    return ret[0][0].properties["value"]


def get(cursor, key):
    cursor.execute(
        """
        MATCH (kv: Kv { key: $key })
        RETURN kv;
    """,
        {"key": key},
    )
    ret = cursor.fetchall()
    return [r[0].properties["value"] for r in ret]


def assert_eq(a, b):
    if a != b:
        print("expected", a, "to be", b)
        assert a == b


def check_commit_fails(conn, failure="conflicting transactions"):
    try:
        conn.commit()
        # should always abort
        return False
    except mgclient.DatabaseError as e:
        if failure not in str(e):
            return False
    return True


def conflicting_update(cursor, key, value) -> bool:
    try:
        update(cursor, key, value)
        return False
    except mgclient.DatabaseError as e:
        if not str(e).startswith("Cannot resolve conflicting transactions"):
            return False
    return True


def select(cursor, key, expected):
    actual = get(cursor, key)
    if actual != expected:
        print("expected key", key, "to have value", expected, "but instead it had value", actual)
        return False
    return True


def select_all(cursor, expected):
    cursor.execute("MATCH (kv: Kv {}) RETURN kv")
    actual = [r[0].properties["value"] for r in cursor.fetchall()]
    if actual != expected:
        print(f"expected values {expected}, but instead it had values {actual}")
        return False
    return True


"""
    G0: Write Cycles (dirty writes)
"""


def g0(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    update(cursor_1, 1, 11)
    # We use pessimistic transactional approach in in-memory storage mode so we fail early
    if not conflicting_update(cursor_2, 1, 12):
        return False, "X g0 test failed"
    update(cursor_1, 2, 21)
    c1.commit()

    get(cursor_1, 1)
    get(cursor_1, 2)

    # if the above write to 1:12 was
    # able to be staged without throwing
    # an exception, these lines would be
    # necessary. but the interactive txn
    # from c2 is already aborted before
    # this point, so there's nothing we
    # need to do here.
    # update(cursor_2, 2, 22)
    # check_commit_fails(c2)

    if not select(cursor_1, 1, [11]):
        return False, "X g0 test failed"
    if not select(cursor_1, 2, [21]):
        return False, "X g0 test failed"

    if not select(cursor_2, 1, [11]):
        return False, "X g0 test failed"
    if not select(cursor_2, 2, [21]):
        return False, "X g0 test failed"

    c1.commit()
    c2.commit()

    return True, "✓ g0 test passed"


"""
    G1a: Aborted Reads (dirty reads, cascaded aborts)
"""


def g1a(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    update(cursor_1, 1, 101)
    if not select(cursor_2, 1, [10]):
        return False, "X g1a test failed"
    c1.rollback()

    if not select(cursor_2, 1, [10]):
        return False, "X g1a test failed"
    c2.commit()

    return True, "✓ g1a test passed"


"""
    G1b: Intermediate Reads (dirty reads)
"""


def g1b(c1, c2, global_isolation_level: str):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    update(cursor_1, 1, 101)
    if not select(cursor_2, 1, [10]):
        return False, "X g1b test failed"
    update(cursor_1, 1, 11)
    c1.commit()

    if global_isolation_level == "READ_COMMITTED":
        if not select(cursor_2, 1, [11]):
            return False, "X g1b test failed"
    else:
        if not select(cursor_2, 1, [10]):  # For SI should pass, for READ_UNCOMMITTED, it should fail
            return False, "X g1b test failed"

    c2.commit()

    return True, "✓ g1b test passed"


"""
    G1c: Circular Information Flow (dirty reads)
"""


def g1c(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    update(cursor_1, 1, 11)
    update(cursor_2, 2, 22)

    if not select(cursor_1, 2, [20]):
        return False, "X g1c test failed"
    if not select(cursor_2, 1, [10]):
        return False, "X g1c test failed"

    c1.commit()
    c2.commit()

    return True, "✓ g1c test passed"


"""
    G1-predA: Non-atomic Predicate-based Reads.

"""


def g1_predA(c1, c2, global_isolation_level: str):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    if not select_all(cursor_1, [10, 20]):
        return False, "X g1_predA test failed"

    update(cursor_2, 1, 100)
    update(cursor_2, 2, 200)
    c2.commit()

    if global_isolation_level == "READ_COMMITTED":
        if not select_all(cursor_1, [100, 200]):
            return False, "X g1_predA test failed"
    else:
        if not select_all(cursor_1, [10, 20]):
            return False, "X g1_predA test failed"

    c1.commit()

    return True, "✓ g1_predA test passed"


"""
    G1-predA: Non-atomic Predicate-based Reads.

"""


def g1_predB(c1, c2, global_isolation_level: str):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    execute_and_fetch_all(cursor_1, "CREATE (n:Node {value: 30})")
    c1.commit()
    if not select_all(cursor_1, [10, 20]):
        return False, "X g1_predB test failed"

    if not execute_and_fetch_all(cursor_1, "MATCH (n:Node) RETURN n")[0][0].properties["value"] == 30:
        return False, "X g1_predB test failed"

    update(cursor_2, 1, 100)
    update(cursor_2, 2, 200)
    execute_and_fetch_all(cursor_2, "MATCH (n:Node) SET n.value = 300 RETURN n")
    c2.commit()

    if global_isolation_level == "READ_COMMITTED":
        if not select_all(cursor_1, [100, 200]):
            return False, "X g1_predB test failed"
        if not execute_and_fetch_all(cursor_1, "MATCH (n:Node) RETURN n")[0][0].properties["value"] == 300:
            return False, "X g1_predB test failed"
    else:
        if not select_all(cursor_1, [10, 20]):
            return False, "X g1_predB test failed"
        if not execute_and_fetch_all(cursor_1, "MATCH (n:Node) RETURN n")[0][0].properties["value"] == 30:
            return False, "X g1_predB test failed"

    c1.commit()

    return True, "✓ g1_predB test passed"


"""
    OTV: Observed Transaction Vanishes
"""


def otv(c1, c2, c3):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()
    cursor_3 = c3.cursor()

    update(cursor_1, 1, 11)
    update(cursor_1, 2, 19)
    if not conflicting_update(cursor_2, 1, 12):
        return False, "X otv test failed"

    c1.commit()

    if not select(cursor_3, 1, [11]):
        return False, "X otv test failed"
    if not select(cursor_3, 2, [19]):
        return False, "X otv test failed"

    # cursor_2 update not required due to its early-abort above

    if not select(cursor_3, 1, [11]):
        return False, "X otv test failed"
    if not select(cursor_3, 2, [19]):
        return False, "X otv test failed"

    c3.commit()

    return True, "✓ otv test passed"


"""
    Predicate-Many-Preceders
"""


def pmp(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    if not select(cursor_1, 3, []):
        return False, "X pmp test failed"
    update(cursor_2, 3, 30)
    c2.commit()

    if not select(cursor_1, 3, []):
        return False, "X pmp test failed"
    c1.commit()

    return True, "✓ pmp test passed"


"""
    Predicate-Many-Preceders (write)
"""


def pmp_write(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    cursor_1.execute("MATCH (kv: Kv {}) SET kv.value = kv.value + 10 RETURN kv")

    if not select(cursor_2, 1, [10]):
        return False, "X pmp_write test failed"
    if not select(cursor_2, 2, [20]):
        return False, "X pmp_write test failed"

    try:
        cursor_2.execute("MATCH (kv:Kv { value: 20 }) DETACH DELETE kv;")
        return False, "X pmp_write test failed"
    except mgclient.DatabaseError as e:
        if "conflicting transactions" not in str(e):
            return False, "X pmp_write test failed"

    c1.commit()

    if not select(cursor_1, 1, [20]):
        return False, "X pmp_write test failed"
    if not select(cursor_1, 2, [30]):
        return False, "X pmp_write test failed"

    c1.commit()

    return True, "✓ pmp_write test passed"


"""
    P4: Lost Update
"""


def p4(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    if not select(cursor_1, 1, [10]):
        return False, "X p4 test failed"
    if not select(cursor_2, 1, [10]):
        return False, "X p4 test failed"

    update(cursor_1, 1, 11)
    if not conflicting_update(cursor_2, 1, 11):
        return False, "X p4 test failed"

    c1.commit()

    return True, "✓ p4 test passed"


"""
    G-single: Single Anti-dependency Cycles (read skew)
"""


def g_single(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    if not select(cursor_1, 1, [10]):
        return False, "X g_single test failed"
    if not select(cursor_2, 1, [10]):
        return False, "X g_single test failed"
    if not select(cursor_2, 2, [20]):
        return False, "X g_single test failed"

    update(cursor_2, 1, 12)
    update(cursor_2, 2, 18)
    c2.commit()

    if not select(
        cursor_1, 2, [20]
    ):  # If this wouldn't be 20, we would have a cycle between T1 and T2.  T1-rw->T2-wr->T1
        return False, "X g_single test failed"
    c1.commit()

    return True, "✓ g_single test passed"


"""
    G-single: Single Anti-dependency Cycles (read skew) (dependencies)
"""


def g_single_dependencies(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    cursor_1.execute("MATCH (kv: Kv {}) WHERE kv.value % 5 = 0 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_1.fetchall()]
    if found != [10, 20]:
        return False, "X g_single_dependencies test failed"

    cursor_2.execute("MATCH (kv: Kv { value: 10 }) SET kv.value = 12 RETURN kv")
    c2.commit()

    cursor_1.execute(
        "MATCH (kv: Kv {}) WHERE kv.value % 3 = 0 RETURN kv"
    )  # If we would see here 12, we would have a cycle between T1 and T2.  T1-rw->T2-wr->T1
    found = [r[0].properties["value"] for r in cursor_1.fetchall()]
    if found != []:
        return False, "X g_single_dependencies test failed"

    c1.commit()

    return True, "✓ g_single_dependencies test passed"


"""
    G-single: Single Anti-dependency Cycles (read skew) (write 1)
"""


def g_single_write_1(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    if not select(cursor_1, 1, [10]):
        return False, "X g_single_write_1 test failed"

    if not select_all(cursor_2, [10, 20]):
        return False, "X g_single_write_1 test failed"

    update(cursor_2, 1, 12)
    update(cursor_2, 2, 18)
    c2.commit()

    try:
        cursor_1.execute("MATCH (kv:Kv { value: 20 }) DETACH DELETE kv;")
        return False, "X g_single_write_1 test failed"
    except mgclient.DatabaseError as e:
        if "conflicting transactions" not in str(e):
            return False, "X g_single_write_1 test failed"

    if not select_all(cursor_2, [12, 18]):
        return False, "X g_single_write_1 test failed"

    c2.commit()

    return True, "✓ g_single_write_1 test passed"


"""
    G-single: Single Anti-dependency Cycles (read skew) (write 2)
"""


def g_single_write_2(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    if not select(cursor_1, 1, [10]):
        return False, "X g_single_write_2 test failed"
    if not select_all(cursor_2, [10, 20]):
        return False, "X g_single_write_2 test failed"

    update(cursor_2, 1, 12)

    cursor_1.execute("MATCH (kv:Kv { value: 20 }) DETACH DELETE kv;")

    if not conflicting_update(cursor_2, 2, 18):
        return False, "X g_single_write_2 test failed"

    c1.rollback()
    # c2.commit()

    return True, "✓ g_single_write_2 test passed"


"""
    G2-item: Item Anti-dependency Cycles (write skew on disjoint read)
"""


def g2_item(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    cursor_1.execute("MATCH (kv: Kv {}) WHERE kv.key = 1 OR kv.key = 2 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_1.fetchall()]
    if found != [10, 20]:
        return False, "X g2_item test failed"

    cursor_2.execute("MATCH (kv: Kv {}) WHERE kv.key = 1 OR kv.key = 2 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_2.fetchall()]
    if found != [10, 20]:
        return False, "X g2_item test failed"

    update(cursor_1, 1, 11)
    update(cursor_2, 2, 21)

    c1.commit()

    if not check_commit_fails(c2):
        return (
            False,
            """
X g2_item test failed - database exhibits write skew -
writes based on invalidated reads should have failed,
causing repeatable read (PL-2.99) and serializability (PL-3)
to fail to be achieved""",
        )

    return True, "✓ g2_item test passed"


"""
    G2: Anti-Dependency Cycles (write skew on predicate read)
"""


def g2(c1, c2, *_):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    cursor_1.execute("MATCH (kv: Kv {}) WHERE kv.value % 3 = 0 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_1.fetchall()]
    if found != []:
        return False, "X g2 test failed"

    cursor_2.execute("MATCH (kv: Kv {}) WHERE kv.value % 3 = 0 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_2.fetchall()]
    if found != []:
        return False, "X g2 test failed"

    update(cursor_1, 3, 30)
    update(cursor_2, 4, 42)

    c1.commit()

    if not check_commit_fails(c2):
        return (
            False,
            """
X g2 test failed - database exhibits write skew on predicate read -,
concurrent transactions that should have caused a predicate read to,
return data in one transaction actually returned nothing in both.,
Both transactions committed, but one of them should have failed due,
to having a predicate invalidated""",
        )

    return True, "✓ g2 test passed"


"""
    G2: Anti-Dependency Cycles (write skew on predicate read) (two edges)
"""


def g2_two_edges(c1, c2, c3):
    cursor_1 = c1.cursor()
    if not select_all(cursor_1, [10, 20]):
        return False, "X g2_two_edges test failed"

    cursor_2 = c2.cursor()
    cursor_2.execute("MATCH (kv: Kv { key: 2 }) SET kv.value = kv.value + 5 RETURN kv;")
    found = [r[0].properties["value"] for r in cursor_2.fetchall()]
    if found != [25]:
        return False, "X g2_two_edges test failed"

    c2.commit()

    cursor_3 = c3.cursor()
    if not select_all(cursor_3, [10, 25]):
        return False, "X g2_two_edges test failed"
    c3.commit()

    if not conflicting_update(cursor_1, 1, 0):
        c3.rollback()
        return (
            False,
            """
X g2_two_edges test failed: database exhibits write skew on predicate read -
a transaction's read set was invalidated in two concurrent transactions,
and it should have failed to commit if we want to be serializable""",
        )

    return True, "✓ g2_two_edges test passed"


def two_txns_phenomenon(phen_func, global_isolation_level):
    c1 = mgclient.connect(host=args.host, port=args.port)
    c1.autocommit = False
    c2 = mgclient.connect(host=args.host, port=args.port)
    c2.autocommit = False

    setup(c1)
    res, info = phen_func(c1, c2, global_isolation_level)
    print(info)

    c1.close()
    c2.close()

    return res


def three_txns_phenomenon(phen_func):
    c1 = mgclient.connect(host=args.host, port=args.port)
    c1.autocommit = False
    c2 = mgclient.connect(host=args.host, port=args.port)
    c2.autocommit = False
    c3 = mgclient.connect(host=args.host, port=args.port)
    c3.autocommit = False

    setup(c1)
    res, info = phen_func(c1, c2, c3)
    print(info)

    c1.close()
    c2.close()
    c3.close()

    return res


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--host", default="127.0.0.1")
    parser.add_argument("-p", "--port", default=7687)
    args = parser.parse_args()

    info_conn = mgclient.connect(host=args.host, port=args.port)
    info_conn.autocommit = True
    info_cursor = info_conn.cursor()
    res = execute_and_fetch_all(info_cursor, "show storage info")
    res = {r[0]: r[1] for r in res}
    global_isolation_level = res["global_isolation_level"]
    print(f"Database started with global isolation level set to: {global_isolation_level}")
    info_conn.close()

    g0 = two_txns_phenomenon(g0, global_isolation_level)
    g1a = two_txns_phenomenon(g1a, global_isolation_level)
    g1b = two_txns_phenomenon(g1b, global_isolation_level)
    g1c = two_txns_phenomenon(g1c, global_isolation_level)
    g1_predA = two_txns_phenomenon(g1_predA, global_isolation_level)
    g1_predB = two_txns_phenomenon(g1_predB, global_isolation_level)
    otv = three_txns_phenomenon(otv)
    pmp = two_txns_phenomenon(pmp, global_isolation_level)
    pmp_write = two_txns_phenomenon(pmp_write, global_isolation_level)
    p4 = two_txns_phenomenon(p4, global_isolation_level)
    g_single = two_txns_phenomenon(g_single, global_isolation_level)
    g_single_dependencies = two_txns_phenomenon(g_single_dependencies, global_isolation_level)
    g_single_write_1 = two_txns_phenomenon(g_single_write_1, global_isolation_level)
    g_single_write_2 = two_txns_phenomenon(g_single_write_2, global_isolation_level)
    g2_item = two_txns_phenomenon(g2_item, global_isolation_level)
    g2 = two_txns_phenomenon(g2, global_isolation_level)
    g2_two_edges = three_txns_phenomenon(g2_two_edges)

    # Check Isolation Levels
    g1 = all([g1a, g1b, g1c])
    repeatable_read = all([g1, g2_item])
    snapshot_isolation = all(
        [g0, g1, otv, pmp, pmp_write, p4, g_single, g_single_dependencies, g_single_write_1, g_single_write_2]
    )
    full_serializability = all([snapshot_isolation, g2_item, g2, g2_two_edges])

    print("")

    print("Isolation levels that Memgraph supports: ")
    print(f"PL-2: {g1}")
    print(f"PL-2': {g1 and g1_predA}")
    print(f"PL-2'': {g1 and g1_predB}")
    print("consistent view (PL-2+):", g1 and g_single)
    print("snapshot isolation (PL-SI):", snapshot_isolation)
    print("repeatable read (PL-2.99):", repeatable_read)
    print("full serializability (PL-3):", full_serializability)

    print("")

    print("more information about these anomalies can be found at:")
    print("\thttps://github.com/ept/hermitage")
    print("\thttps://pmg.csail.mit.edu/papers/adya-phd.pdf")
