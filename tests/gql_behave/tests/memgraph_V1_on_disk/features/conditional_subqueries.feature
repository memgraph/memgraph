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

Feature: Conditional subqueries
    CALL (...) { WHEN p THEN body [WHEN ...] [ELSE body] } runs the first branch whose predicate holds.

    Scenario: A unit body keeps every row and runs only the matching branch
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i = 1 THEN CREATE (:T {v: i}) WHEN i = 2 THEN CREATE (:T {v: i * 10}) }
            RETURN i
            ORDER BY i
            """
        Then the result should be:
            | i |
            | 1 |
            | 2 |
            | 3 |
        When executing control query:
            """
            MATCH (t:T) RETURN t.v AS v ORDER BY v
            """
        Then the result should be:
            | v  |
            | 1  |
            | 20 |

    Scenario: A write in the first branch is visible after the CALL
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i
            CALL (i) { WHEN i < 3 THEN CREATE (:W) RETURN 1 AS x ELSE RETURN 2 AS x }
            MATCH (w:W)
            RETURN i, x, count(w) AS c
            ORDER BY i
            """
        Then the result should be:
            | i | x | c |
            | 1 | 1 | 2 |
            | 2 | 1 | 2 |
            | 3 | 2 | 2 |

    Scenario: The folds of an expression body with WHEN branches
        Given an empty graph
        And having executed
            """
            CREATE (:P {v: 1})-[:R]->(:Q {v: 10}), (:P {v: 2})
            """
        When executing query:
            """
            MATCH (p:P)
            RETURN p.v AS v,
                   EXISTS { WHEN p.v = 1 THEN MATCH (p)-[:R]->(q) RETURN q } AS e,
                   COUNT { WHEN p.v = 1 THEN MATCH (p)-[:R]->(q) RETURN q ELSE RETURN 0 AS q } AS c,
                   COLLECT { WHEN p.v = 1 THEN MATCH (p)-[:R]->(q) RETURN q.v AS q } AS l
            ORDER BY v
            """
        Then the result should be:
            | v | e     | c | l    |
            | 1 | true  | 1 | [10] |
            | 2 | false | 1 | []   |
