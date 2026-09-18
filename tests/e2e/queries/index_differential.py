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

"""No query may answer differently for having an index.

An index is a performance decision, so a plan that swaps a filter for a scan has
to hand back the rows the filter kept. The two sides read different relations to
decide that, and each place they can disagree is a wrong answer rather than a
slow one.

This asks the question directly: the same predicate over the same data, once
with an index and once without, and the two counts compared. It varies the value
within a type as well as across types, because a disagreement can turn on which
value of a type is asked about rather than on the type alone.
"""

import sys

import pytest
from common import memgraph

# One entry per stored type, with several values inside each so that a
# value-level disagreement is reachable. The awkward values are deliberate: the
# ends of the integer range, an infinity, a NaN, an empty string, an empty
# container, and a list holding more than one type.
VALUES_BY_TYPE = {
    "boolean": ["true", "false"],
    # The smallest integer is absent because it has no literal: the parser reads
    # the minus as an operator and the digits alone are out of range.
    "integer": ["-9223372036854775807", "-1", "0", "1", "9007199254740993", "9223372036854775807"],
    "double": ["-1.0 / 0.0", "-1.5", "0.0", "1.5", "1.0 / 0.0", "sqrt(-1)"],
    "string": ["''", "'a'", "'ab'", "'b'"],
    "date": ["date('1970-01-01')", "date('2020-02-29')", "date('9999-12-31')"],
    "local time": ["localTime('00:00:00')", "localTime('12:34:56')", "localTime('23:59:59')"],
    "local date time": ["localDateTime('1970-01-01T00:00:00')", "localDateTime('2020-01-01T12:00:00')"],
    "duration": ["duration('PT1S')", "duration('P1D')", "duration('P100D')"],
    "zoned date time": ["datetime('1970-01-01T00:00:00+00:00')", "datetime('2020-01-01T12:00:00+02:00')"],
    "list": ["[]", "[1]", "[1, 2]", "[1, 'a']", "['a']"],
    "map": ["{}", "{a: 1}", "{a: 2}"],
    "point": ["point({x: 0, y: 0})", "point({x: 1, y: 2})", "point({x: sqrt(-1), y: 1})"],
}

# A column holding more than one type, which is where the two relations are most
# likely to part: each has its own rule for a pair they cannot both place.
MIXED_VALUES = [
    "1",
    "2.5",
    "'a'",
    "true",
    "[1]",
    "{a: 1}",
    "date('2020-01-01')",
    "duration('P1D')",
    "point({x: 1, y: 2})",
    "sqrt(-1)",
]

ORDERED_COMPARISONS = ["<", "<=", ">", ">="]


def _probes(values):
    """Every predicate worth asking of a column holding these values."""
    probes = [f"{op} {value}" for value in values for op in ORDERED_COMPARISONS]
    probes += [f"= {value}" for value in values]
    probes.append("IS NOT NULL")
    probes.append("IN [{}]".format(", ".join(values)))
    return probes


def _counts(memgraph, probes):
    """The node count and the edge count for each probe, in one list."""
    answers = []
    for probe in probes:
        for query in (
            f"MATCH (n:D) WHERE n.p {probe} RETURN count(n) AS c;",
            f"MATCH ()-[r:D]->() WHERE r.p {probe} RETURN count(r) AS c;",
        ):
            rows = list(memgraph.execute_and_fetch(query))
            answers.append(rows[0]["c"] if rows else 0)
    return answers


def _load(memgraph, values):
    """A node and an edge carrying each value, so both scans are asked."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    for value in values:
        memgraph.execute(f"CREATE (:D {{p: {value}}});")
    memgraph.execute("CREATE (:From), (:To);")
    for value in values:
        memgraph.execute(f"MATCH (a:From), (b:To) CREATE (a)-[:D {{p: {value}}}]->(b);")


def _answers_agree(memgraph, values):
    """Ask every probe with no index, then with one, and hand back both."""
    _load(memgraph, values)
    probes = _probes(values)

    without_index = _counts(memgraph, probes)

    memgraph.execute("CREATE INDEX ON :D(p);")
    memgraph.execute("CREATE EDGE INDEX ON :D(p);")

    with_index = _counts(memgraph, probes)

    memgraph.execute("DROP INDEX ON :D(p);")
    memgraph.execute("DROP EDGE INDEX ON :D(p);")

    return probes, without_index, with_index


def _report(probes, without_index, with_index):
    """Name the probe that disagreed rather than only the lists."""
    lines = []
    for i, probe in enumerate(probes):
        for offset, kind in ((0, "node"), (1, "edge")):
            at = i * 2 + offset
            if without_index[at] != with_index[at]:
                lines.append(f"  p {probe} ({kind}): {without_index[at]} without an index, {with_index[at]} with one")
    return "\n".join(lines)


@pytest.mark.parametrize("type_name", sorted(VALUES_BY_TYPE))
def test_an_index_over_one_type_answers_as_the_filter_does(memgraph, type_name):
    """Every predicate over a column of one type, asked both ways."""
    values = VALUES_BY_TYPE[type_name]
    probes, without_index, with_index = _answers_agree(memgraph, values)

    assert with_index == without_index, f"an index changed the answer for {type_name}:\n" + _report(
        probes, without_index, with_index
    )

    # Non-vacuous: a run where every probe kept no row would pass while asking
    # nothing, and an empty column is exactly how that happens.
    assert any(count > 0 for count in without_index), f"no probe over {type_name} kept a row"


def test_an_index_over_a_column_of_many_types_answers_as_the_filter_does(memgraph):
    """The same, over a column holding a value of each type at once."""
    probes, without_index, with_index = _answers_agree(memgraph, MIXED_VALUES)

    assert with_index == without_index, "an index changed the answer over a column of many types:\n" + _report(
        probes, without_index, with_index
    )
    assert any(count > 0 for count in without_index)


def test_an_index_on_two_properties_answers_as_the_filter_does(memgraph):
    """A composite index, with its trailing level left unbounded.

    The fence a scan stops at when a level carries no bound of its own has to sit
    above every value that level could hold, and the trailing values here run
    through every type so that the fence is asked about each of them."""
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    for i, value in enumerate(MIXED_VALUES):
        memgraph.execute(f"CREATE (:C {{a: {i}, b: {value}}});")

    queries = [
        "MATCH (n:C) WHERE n.a >= 0 RETURN count(n) AS c;",
        "MATCH (n:C) WHERE n.a > 2 RETURN count(n) AS c;",
        "MATCH (n:C) WHERE n.a >= 0 AND n.a < 100 RETURN count(n) AS c;",
        "MATCH (n:C) WHERE n.a >= 0 RETURN count(n.b) AS c;",
    ]

    def counts():
        return [list(memgraph.execute_and_fetch(q))[0]["c"] for q in queries]

    without_index = counts()
    memgraph.execute("CREATE INDEX ON :C(a, b);")
    with_index = counts()
    memgraph.execute("DROP INDEX ON :C(a, b);")

    assert with_index == without_index, f"a composite index changed the answer: {without_index} vs {with_index}"
    assert without_index[0] == len(MIXED_VALUES)


def test_the_string_predicates_answer_as_the_filter_does(memgraph):
    """The three that read a search term rather than a bound, on both scans."""
    values = ["''", "'a'", "'ab'", "'abc'", "'b'", "'zzz'"]
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    for value in values:
        memgraph.execute(f"CREATE (:D {{p: {value}}});")
    memgraph.execute("CREATE (:From), (:To);")
    for value in values:
        memgraph.execute(f"MATCH (a:From), (b:To) CREATE (a)-[:D {{p: {value}}}]->(b);")

    probes = [
        "STARTS WITH 'a'",
        "STARTS WITH ''",
        "CONTAINS 'b'",
        "CONTAINS ''",
        "ENDS WITH 'c'",
    ]

    without_index = _counts(memgraph, probes)
    memgraph.execute("CREATE INDEX ON :D(p);")
    memgraph.execute("CREATE EDGE INDEX ON :D(p);")
    with_index = _counts(memgraph, probes)
    memgraph.execute("DROP INDEX ON :D(p);")
    memgraph.execute("DROP EDGE INDEX ON :D(p);")

    assert with_index == without_index, "an index changed the answer for a string predicate:\n" + _report(
        probes, without_index, with_index
    )
    assert any(count > 0 for count in without_index)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
