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
"""

import argparse
import mgclient


def setup(conn):
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("CREATE CONSTRAINT ON (kv: Kv) ASSERT kv.key IS UNIQUE;")
    conn.commit()
    conn.autocommit = False

    cursor = conn.cursor()
    cursor.execute("MATCH (kv:Kv) DETACH DELETE kv;")
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


def assert_commit_fails(conn, failure="conflicting transactions"):
    try:
        conn.commit()
        # should always abort
        print("expected transaction to fail with", failure, "but it incorrectly committed successfully")
        assert False
    except mgclient.DatabaseError as e:
        assert failure in str(e), "expected exception containing text {failure} but instead it was {e}"


def conflicting_update(cursor, key, value):
    try:
        update(cursor, key, value)
        assert False
    except mgclient.DatabaseError as e:
        assert str(e).startswith("Cannot resolve conflicting transactions")


def select(cursor, key, expected):
    actual = get(cursor, key)
    if actual != expected:
        print("expected key", key, "to have value", expected, "but instead it had value", actual)
        assert False


def select_all(cursor, expected):
    cursor.execute("MATCH (kv: Kv {}) RETURN kv")
    actual = [r[0].properties["value"] for r in cursor.fetchall()]
    assert actual == expected, "expected values {expected}, but instead it had values {actual}"


"""
    G0: Write Cycles (dirty writes)
"""


def g0(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    update(cursor_1, 1, 11)
    conflicting_update(cursor_2, 1, 12)
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
    # assert_commit_fails(c2)

    select(cursor_1, 1, [11])
    select(cursor_1, 2, [21])

    select(cursor_2, 1, [11])
    select(cursor_2, 2, [21])

    c1.commit()
    c2.commit()

    print("✓ g0 test passed")
    return True


"""
    G1a: Aborted Reads (dirty reads, cascaded aborts)
"""


def g1a(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    update(cursor_1, 1, 101)
    select(cursor_2, 1, [10])
    c1.rollback()

    select(cursor_2, 1, [10])
    c2.commit()

    print("✓ g1a test passed")
    return True


"""
    G1b: Intermediate Reads (dirty reads)
"""


def g1b(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    update(cursor_1, 1, 101)
    select(cursor_2, 1, [10])
    update(cursor_1, 1, 11)
    c1.commit()

    select(cursor_2, 1, [10])
    c2.commit()

    print("✓ g1b test passed")
    return True


"""
    G1c: Circular Information Flow (dirty reads)
"""


def g1c(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    update(cursor_1, 1, 11)
    update(cursor_2, 2, 22)

    select(cursor_1, 2, [20])
    select(cursor_2, 1, [10])

    c1.commit()
    c2.commit()

    print("✓ g1c test passed")
    return True


"""
    OTV: Observed Transaction Vanishes
"""


def otv(c1, c2, c3):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()
    cursor_3 = c3.cursor()

    update(cursor_1, 1, 11)
    update(cursor_1, 2, 19)
    conflicting_update(cursor_2, 1, 12)

    c1.commit()

    select(cursor_3, 1, [11])
    select(cursor_3, 2, [19])

    # cursor_2 update not required due to its early-abort above

    select(cursor_3, 1, [11])
    select(cursor_3, 2, [19])

    c3.commit()

    print("✓ otv test passed")
    return True


"""
    Predicate-Many-Preceders
"""


def pmp(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    select(cursor_1, 3, [])
    update(cursor_2, 3, 30)
    c2.commit()

    select(cursor_1, 3, [])
    c1.commit()

    print("✓ pmp test passed")
    return True


"""
    Predicate-Many-Preceders (write)
"""


def pmp_write(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    cursor_1.execute("MATCH (kv: Kv {}) SET kv.value = kv.value + 10 RETURN kv")

    select(cursor_2, 1, [10])
    select(cursor_2, 2, [20])

    try:
        cursor_2.execute("MATCH (kv:Kv { value: 20 }) DETACH DELETE kv;")
        assert False
    except mgclient.DatabaseError as e:
        assert "conflicting transactions" in str(e)

    c1.commit()

    select(cursor_1, 1, [20])
    select(cursor_1, 2, [30])

    c1.commit()

    print("✓ pmp_write test passed")
    return True


"""
    P4: Lost Update
"""


def p4(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    select(cursor_1, 1, [10])
    select(cursor_2, 1, [10])

    update(cursor_1, 1, 11)
    conflicting_update(cursor_2, 1, 11)

    c1.commit()

    print("✓ p4 test passed")
    return True


"""
    G-single: Single Anti-dependency Cycles (read skew)
"""


def g_single(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    select(cursor_1, 1, [10])
    select(cursor_2, 1, [10])
    select(cursor_2, 2, [20])

    update(cursor_2, 1, 12)
    update(cursor_2, 2, 18)
    c2.commit()

    select(cursor_1, 2, [20])
    c1.commit()

    print("✓ g_single test passed")
    return True


"""
    G-single: Single Anti-dependency Cycles (read skew) (dependencies)
"""


def g_single_dependencies(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    cursor_1.execute("MATCH (kv: Kv {}) WHERE kv.value % 5 = 0 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_1.fetchall()]
    assert found == [10, 20]

    cursor_2.execute("MATCH (kv: Kv { value: 10 }) SET kv.value = 12 RETURN kv")
    c2.commit()

    cursor_1.execute("MATCH (kv: Kv {}) WHERE kv.value % 3 = 0 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_1.fetchall()]
    assert found == []

    c1.commit()

    print("✓ g_single_dependencies test passed")
    return True


"""
    G-single: Single Anti-dependency Cycles (read skew) (write 1)
"""


def g_single_write_1(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    select(cursor_1, 1, [10])

    select_all(cursor_2, [10, 20])

    update(cursor_2, 1, 12)
    update(cursor_2, 2, 18)
    c2.commit()

    try:
        cursor_1.execute("MATCH (kv:Kv { value: 20 }) DETACH DELETE kv;")
        assert False
    except mgclient.DatabaseError as e:
        assert "conflicting transactions" in str(e)

    select_all(cursor_2, [12, 18])

    c2.commit()

    print("✓ g_single_write_1 test passed")
    return True


"""
    G-single: Single Anti-dependency Cycles (read skew) (write 2)
"""


def g_single_write_2(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    select(cursor_1, 1, [10])
    select_all(cursor_2, [10, 20])

    update(cursor_2, 1, 12)

    cursor_1.execute("MATCH (kv:Kv { value: 20 }) DETACH DELETE kv;")

    conflicting_update(cursor_2, 2, 18)

    c1.rollback()
    # c2.commit()

    print("✓ g_single_write_2 test passed (although the abort rate is pessimistically high)")
    return True


"""
    G2-item: Item Anti-dependency Cycles (write skew on disjoint read)
"""


def g2_item(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    cursor_1.execute("MATCH (kv: Kv {}) WHERE kv.key = 1 OR kv.key = 2 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_1.fetchall()]
    assert found == [10, 20]

    cursor_2.execute("MATCH (kv: Kv {}) WHERE kv.key = 1 OR kv.key = 2 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_2.fetchall()]
    assert found == [10, 20]

    update(cursor_1, 1, 11)
    update(cursor_2, 2, 21)

    c1.commit()

    try:
        assert_commit_fails(c2)
        print("✓ g2_item test passed")
        return True
    except:
        print(
            "X g2_item test failed - database exhibits write skew -",
            "writes based on invalidated reads should have failed,",
            "causing repeatable read (PL-2.99) and serializability (PL-3)",
            "to fail to be achieved",
        )
        return False


"""
    G2: Anti-Dependency Cycles (write skew on predicate read)
"""


def g2(c1, c2):
    cursor_1 = c1.cursor()
    cursor_2 = c2.cursor()

    cursor_1.execute("MATCH (kv: Kv {}) WHERE kv.value % 3 = 0 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_1.fetchall()]
    assert found == []

    cursor_2.execute("MATCH (kv: Kv {}) WHERE kv.value % 3 = 0 RETURN kv")
    found = [r[0].properties["value"] for r in cursor_2.fetchall()]
    assert found == []

    update(cursor_1, 3, 30)
    update(cursor_2, 4, 42)

    c1.commit()

    try:
        assert_commit_fails(c2)
        print("✓ g2 test passed")
        return True
    except:
        print(
            "X g2 test failed - database exhibits write skew on predicate read -",
            "concurrent transactions that should have caused a predicate read to",
            "return data in one transaction actually returned nothing in both.",
            "Both transactions committed, but one of them should have failed due",
            "to having a predicate invalidated",
        )
        return False


"""
    G2: Anti-Dependency Cycles (write skew on predicate read) (two edges)
"""


def g2_two_edges(c1, c2, c3):
    cursor_1 = c1.cursor()
    select_all(cursor_1, [10, 20])

    cursor_2 = c2.cursor()
    cursor_2.execute("MATCH (kv: Kv { key: 2 }) SET kv.value = kv.value + 5 RETURN kv;")
    found = [r[0].properties["value"] for r in cursor_2.fetchall()]
    assert found == [25]
    c2.commit()

    cursor_3 = c3.cursor()
    select_all(cursor_3, [10, 25])
    c3.commit()

    try:
        conflicting_update(cursor_1, 1, 0)
        print("✓ g2_two_edges test passed")
        return True
    except:
        c3.rollback()
        print(
            "X g2_two_edges test failed: database exhibits write skew on predicate read -",
            "a transaction's read set was invalidated in two concurrent transactions,"
            "and it should have failed to commit if we want to be serializable",
        )
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--host", default="127.0.0.1")
    parser.add_argument("-p", "--port", default=7687)
    args = parser.parse_args()

    c1 = mgclient.connect(host=args.host, port=args.port)
    c1.autocommit = False
    c2 = mgclient.connect(host=args.host, port=args.port)
    c2.autocommit = False
    c3 = mgclient.connect(host=args.host, port=args.port)
    c3.autocommit = False

    setup(c1)
    g0 = g0(c1, c2)

    setup(c1)
    g1a = g1a(c1, c2)

    setup(c1)
    g1b = g1b(c1, c2)

    setup(c1)
    g1c = g1c(c1, c2)

    setup(c1)
    otv = otv(c1, c2, c3)

    setup(c1)
    pmp = pmp(c1, c2)

    setup(c1)
    pmp_write = pmp_write(c1, c2)

    setup(c1)
    p4 = p4(c1, c2)

    setup(c1)
    g_single = g_single(c1, c2)

    setup(c1)
    g_single_dependencies = g_single_dependencies(c1, c2)

    setup(c1)
    g_single_write_1 = g_single_write_1(c1, c2)

    setup(c1)
    g_single_write_2 = g_single_write_2(c1, c2)

    setup(c1)
    g2_item = g2_item(c1, c2)

    setup(c1)
    g2 = g2(c1, c2)

    setup(c1)
    g2_two_edges = g2_two_edges(c1, c2, c3)

    g1 = all([g1a, g1b, g1c])
    repeatable_read = all([g1, g2_item])
    snapshot_isolation = all(
        [g0, g1, otv, pmp, pmp_write, p4, g_single, g_single_dependencies, g_single_write_1, g_single_write_2]
    )
    full_serializability = all([snapshot_isolation, g2_item, g2, g2_two_edges])

    print("")

    print("results:")
    print("consistent view (PL-2+):", g1 and g_single)
    print("snapshot isolation (PL-SI):", snapshot_isolation)
    print("repeatable read (PL-2.99):", repeatable_read)
    print("full serializability (PL-3):", full_serializability)

    print("")

    print("more information about these anomalies can be found at:")
    print("\thttps://github.com/ept/hermitage")
    print("\thttps://pmg.csail.mit.edu/papers/adya-phd.pdf")
