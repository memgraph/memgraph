# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""A query answers the same whether or not an index is involved.

An index holds its entries by one relation and a filter reads another, and
where the two part company a scan can return a row a filter would have refused
or pass over one it would have kept. The corpus below is the values that part
them: a null, a NaN, and containers holding either.

Every case runs the same query twice, against a label carrying an index and a
label carrying none, and requires the two to answer alike. Rows are identified
by a key rather than by the value itself, so a NaN in the results does not
defeat the comparison the way it would defeat comparing the values.
"""

import sys

import mgclient
import pytest
from common import cursor, execute_and_fetch_all

# One row per entry: the key that names it, and the value stored under `p`.
#
# Written as Cypher rather than passed as parameters because several of these
# have no Bolt spelling from the client side, and because a literal is what a
# plan can read a type from.
CORPUS = [
    ("int", "1"),
    ("int_other", "2"),
    ("double_whole", "1.0"),
    ("double", "1.5"),
    ("nan", "sqrt(-1)"),
    ("string", "'a'"),
    ("string_other", "'b'"),
    ("bool", "true"),
    ("null", "null"),
    ("list", "[1, 2]"),
    ("list_shorter", "[1]"),
    ("list_with_null", "[1, null]"),
    ("list_with_nan", "[1, sqrt(-1)]"),
    ("nested_list_with_null", "[[null]]"),
    ("map", "{a: 1}"),
    ("map_with_null", "{a: null}"),
    # One stored type carries all four of these and orders them by which kind it
    # holds, while the comparison places no pair drawn from two kinds. They are
    # the values a range fenced to that type admits and a filter refuses, so a
    # column holding more than one kind is where the two part company.
    ("date", "date('2020-01-01')"),
    ("date_later", "date('2024-06-01')"),
    ("local_time", "localTime('12:00:00')"),
    ("local_date_time", "localDateTime('2020-01-01T12:00:00')"),
    ("duration", "duration('P1D')"),
    ("zoned_date_time", "datetime('2020-01-01T12:00:00+01:00')"),
    # Neither is placed by the comparison operators, which answer Null for a
    # pair of them as they do for any two values with no order between them. A
    # stored one therefore has to be passed over by an ordered probe rather
    # than reported as an error.
    ("point_2d", "point({x: 1, y: 2})"),
    ("point_3d", "point({x: 1, y: 2, z: 3})"),
]

# Each reads one of the four relations. `$v` is substituted with a corpus
# spelling, so every probe is run against every stored value.
PROBES = [
    "MATCH (n:{label}) WHERE n.p = {v} RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) WHERE n.p <> {v} RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) WHERE n.p < {v} RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) WHERE n.p <= {v} RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) WHERE n.p > {v} RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) WHERE n.p >= {v} RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) WHERE n.p IN [{v}] RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) WHERE n.p IN [{v}, null] RETURN n.k ORDER BY n.k",
    # Equality again, read where a scan cannot stand in for the filter, so the
    # answer has to come out the same by a different route.
    "MATCH (n:{label}) RETURN n.k, CASE n.p WHEN {v} THEN 1 ELSE 0 END AS m ORDER BY n.k",
]

# These read no value of their own, so they run once rather than per corpus
# entry. The sort ones tie-break on the key so a pair the order places
# alongside one another cannot make the comparison depend on which arrived
# first.
WHOLE_COLUMN_PROBES = [
    "MATCH (n:{label}) WHERE n.p IS NOT NULL RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) WHERE n.p IS NULL RETURN n.k ORDER BY n.k",
    "MATCH (n:{label}) RETURN n.k ORDER BY n.p, n.k",
    "MATCH (n:{label}) RETURN n.k ORDER BY n.p DESC, n.k",
    "MATCH (n:{label}) RETURN n.k ORDER BY n.p, n.k LIMIT 5",
    "MATCH (n:{label}) RETURN n.k ORDER BY n.p, n.k SKIP 3",
    "MATCH (n:{label}) RETURN n.k ORDER BY n.p, n.k SKIP 2 LIMIT 4",
    "MATCH (n:{label}) RETURN count(DISTINCT n.p) AS c",
    # DISTINCT folds by equivalence, which holds two nulls and two NaNs alike
    # where the equality a scan reads decides neither.
    "MATCH (n:{label}) WITH DISTINCT n.p AS p RETURN count(*) AS c",
    "MATCH (n:{label}) RETURN min(n.p) IS NULL AS lo, max(n.p) IS NULL AS hi",
    "MATCH (n:{label}) WITH n.p AS p, count(*) AS c RETURN c ORDER BY c",
    # A join whose key is the property, which matches by the relation a hash
    # table can read rather than the one the comparison writes.
    "MATCH (a:{label}), (b:{label}) WHERE a.p = b.p RETURN a.k, b.k ORDER BY a.k, b.k",
]

INDEXED = "Indexed"
PLAIN = "Plain"


def on_disk(cursor):
    """Whether the server is running the on-disk storage mode."""
    for row in execute_and_fetch_all(cursor, "SHOW STORAGE INFO"):
        if str(row[0]).endswith("storage_mode"):
            return "ON_DISK" in str(row[1])
    return False


# The disk scan reads a range by its bounds alone. A range over lists needs the
# comparison behind it asked of each candidate as well, because two lists are
# ordered where the comparison still declines to answer, and only the in-memory
# index asks it. So a list bound admits rows on disk that the filter refuses.
#
# The disk mode carries fewer features than the in-memory one by design, and
# this is one of them rather than something gone wrong. It is marked here so
# that the case still runs, and reports, if the mode ever gains it.
DISK_IGNORES_THE_RANGE_COMPARISON = "the disk scan does not ask the comparison behind a range over lists"


@pytest.fixture
def populated(cursor):
    """The same rows under two labels, one of which carries an index."""
    for key, value in CORPUS:
        for label in (INDEXED, PLAIN):
            execute_and_fetch_all(cursor, f"CREATE (:{label} {{k: '{key}', p: {value}}})")
    execute_and_fetch_all(cursor, f"CREATE INDEX ON :{INDEXED}(p)")
    yield cursor


def outcome(cursor, query):
    """The rows a query answers with, or the fact that it raised.

    Raising is as much an answer as returning rows, and several of these ask
    for a comparison Cypher does not define. What has to hold is that an index
    does not change which of the two happens, so the error is recorded as
    having happened rather than by its wording: a scan and a filter reach the
    same refusal by different routes and say so in different words.
    """
    try:
        return ("rows", execute_and_fetch_all(cursor, query))
    except mgclient.DatabaseError:
        return ("error", None)


def assert_agrees(cursor, template, **kwargs):
    indexed = outcome(cursor, template.format(label=INDEXED, **kwargs))
    plain = outcome(cursor, template.format(label=PLAIN, **kwargs))
    assert indexed == plain, (
        f"an index changed the answer to `{template.format(label='<label>', **kwargs)}`: "
        f"indexed gave {indexed}, unindexed gave {plain}"
    )


ORDERED_COMPARISONS = ("n.p < ", "n.p <= ", "n.p > ")


@pytest.mark.parametrize("probe", PROBES)
@pytest.mark.parametrize("name,value", CORPUS)
def test_a_probe_answers_alike_with_and_without_an_index(populated, probe, name, value):
    reads_a_range = any(comparison in probe for comparison in ORDERED_COMPARISONS)
    if on_disk(populated) and reads_a_range and value.startswith("["):
        pytest.xfail(DISK_IGNORES_THE_RANGE_COMPARISON)
    assert_agrees(populated, probe, v=value)


@pytest.mark.parametrize("probe", WHOLE_COLUMN_PROBES)
def test_a_whole_column_answers_alike_with_and_without_an_index(populated, probe):
    assert_agrees(populated, probe)


def test_a_stored_nan_is_reachable_through_its_own_index(populated):
    # The case that goes wrong quietly: an index that cannot confirm the entry
    # it holds drops the row rather than reporting anything.
    rows = execute_and_fetch_all(populated, f"MATCH (n:{INDEXED}) WHERE n.p = sqrt(-1) RETURN n.k")
    unindexed = execute_and_fetch_all(populated, f"MATCH (n:{PLAIN}) WHERE n.p = sqrt(-1) RETURN n.k")
    assert rows == unindexed
    # A NaN is equal to nothing, itself included, so neither label answers with
    # the row. It must still be reachable when asked for as a value.
    assert rows == []
    present = execute_and_fetch_all(populated, f"MATCH (n:{INDEXED}) WHERE n.p IS NOT NULL RETURN n.k ORDER BY n.k")
    assert ("nan",) in present, "a stored NaN vanished from its own index"


def test_a_hash_join_pairs_only_what_equality_pairs(cursor):
    """A join keyed by a hash table answers an equality question.

    A hash table can only match by equivalence, which holds two nulls alike and
    two NaNs alike where the equality a query writes holds neither. A join that
    paired them would answer rows the same predicate refuses when it is read as
    a filter.
    """
    for key, value in [
        ("one", "1"),
        ("one_again", "1"),
        ("nan", "sqrt(-1)"),
        ("nan_again", "sqrt(-1)"),
        ("null", "null"),
        ("null_again", "null"),
    ]:
        execute_and_fetch_all(cursor, f"CREATE (:J {{k: '{key}', p: {value}}})")

    plan = [
        row[0] for row in execute_and_fetch_all(cursor, "EXPLAIN MATCH (a:J), (c:J) WHERE c.p = a.p RETURN a.k, c.k")
    ]
    assert any("HashJoin" in step for step in plan), f"this query no longer joins by a hash table: {plan}"

    pairs = execute_and_fetch_all(cursor, "MATCH (a:J), (c:J) WHERE c.p = a.p RETURN a.k, c.k ORDER BY a.k, c.k")
    # Only the two rows holding 1 pair, and each pairs with itself and the
    # other. Nothing holding a NaN or a null appears at all, itself included.
    assert pairs == [
        ("one", "one"),
        ("one", "one_again"),
        ("one_again", "one"),
        ("one_again", "one_again"),
    ], pairs


def test_a_point_index_answers_as_the_filter_does(cursor):
    """A point index is read by a predicate of its own rather than by a range.

    Points carry an order only a sort reads, so the comparison operators refuse
    them and a scan cannot stand in for one. What a point index does answer is
    a distance predicate, and that has to agree with the filter as every other
    scan does.
    """
    if on_disk(cursor):
        pytest.skip("point indexes are not implemented for the on-disk storage mode")

    for label in (INDEXED, PLAIN):
        execute_and_fetch_all(cursor, f"CREATE (:{label} {{k: 'near', loc: point({{x: 1, y: 1}})}})")
        execute_and_fetch_all(cursor, f"CREATE (:{label} {{k: 'far', loc: point({{x: 50, y: 50}})}})")
    execute_and_fetch_all(cursor, f"CREATE POINT INDEX ON :{INDEXED}(loc)")

    for probe in [
        "MATCH (n:{label}) WHERE point.distance(n.loc, point({{x: 1, y: 1}})) < 1 RETURN n.k ORDER BY n.k",
        "MATCH (n:{label}) WHERE point.distance(n.loc, point({{x: 1, y: 1}})) < 1000000 RETURN n.k ORDER BY n.k",
        "MATCH (n:{label}) WHERE n.loc IS NOT NULL RETURN n.k ORDER BY n.k",
    ]:
        assert_agrees(cursor, probe)


def test_a_point_range_answers_null_rather_than_reading_the_order_an_index_keeps(cursor):
    """The comparison operators place no pair of points, so a range over them holds nothing.

    An index does keep points in an order, and a scan reading that order would
    answer a question the comparison declines to.
    """
    for label in (INDEXED, PLAIN):
        execute_and_fetch_all(cursor, f"CREATE (:{label} {{k: 'a', p: point({{x: 1, y: 1}})}})")
    execute_and_fetch_all(cursor, f"CREATE INDEX ON :{INDEXED}(p)")

    assert_agrees(cursor, "MATCH (n:{label}) WHERE n.p < point({{x: 9, y: 9}}) RETURN n.k")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))


def test_a_bound_reading_an_outer_row_is_read_again_for_every_row(cursor):
    """A range over lists is refined by the comparison behind its bounds.

    That comparison carries the values the bounds evaluated to rather than the
    expressions behind them, so one built for the first row and kept would
    answer for every row after it. A bound reading an outer row is what tells
    the two apart, and the unindexed label is the oracle: a filter always reads
    the bound of the row it is on.
    """
    for label in (INDEXED, PLAIN):
        for key, value in [("a", "[1]"), ("b", "[2]"), ("c", "[3]")]:
            execute_and_fetch_all(cursor, f"CREATE (:Outer{label} {{k: '{key}', p: {value}}})")
    execute_and_fetch_all(cursor, "CREATE (:Bound {id: 1, lim: [2]})")
    execute_and_fetch_all(cursor, "CREATE (:Bound {id: 2, lim: [3]})")
    execute_and_fetch_all(cursor, f"CREATE INDEX ON :Outer{INDEXED}(p)")

    probe = "MATCH (m:Bound) MATCH (n:Outer{label}) WHERE n.p < m.lim RETURN m.id, n.k ORDER BY m.id, n.k"
    indexed = execute_and_fetch_all(cursor, probe.format(label=INDEXED))
    plain = execute_and_fetch_all(cursor, probe.format(label=PLAIN))

    assert indexed == plain, f"an index answered a per-row bound differently: {indexed} against {plain}"
    # Named outright, so the case still means something if both sides break.
    assert indexed == [(1, "a"), (2, "a"), (2, "b")]
