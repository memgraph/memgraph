Feature: Conditional queries

    Scenario: Top-level WHEN takes the first branch when its predicate is true
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN RETURN 1 AS x ELSE RETURN 2 AS x
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: Top-level WHEN takes ELSE when the predicate is false
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | b | false |
        When executing query:
            """
            WHEN $b THEN RETURN 1 AS x ELSE RETURN 2 AS x
            """
        Then the result should be:
            | x |
            | 2 |

    Scenario: Top-level WHEN treats a null predicate as false
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | z | null |
        When executing query:
            """
            WHEN $z THEN RETURN 1 AS x ELSE RETURN 2 AS x
            """
        Then the result should be:
            | x |
            | 2 |

    Scenario: Top-level WHEN without ELSE and no true predicate returns no rows
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | b | false |
        When executing query:
            """
            WHEN $b THEN RETURN 1 AS x
            """
        Then the result should be empty

    Scenario: Top-level WHEN runs only the first true branch
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN RETURN 1 AS x WHEN $a THEN RETURN 2 AS x ELSE RETURN 3 AS x
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: Top-level WHEN takes a later branch when the earlier predicates are false
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true  |
            | b | false |
        When executing query:
            """
            WHEN $b THEN RETURN 1 AS x WHEN $a THEN RETURN 2 AS x ELSE RETURN 3 AS x
            """
        Then the result should be:
            | x |
            | 2 |

    Scenario: Top-level WHEN with braced branches
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN { RETURN 1 AS x } ELSE { RETURN 2 AS x }
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: Top-level WHEN branch yields every row of its MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN MATCH (n:P) RETURN n.n AS x ELSE RETURN 0 AS x
            """
        Then the result should be:
            | x |
            | 1 |
            | 2 |
            | 4 |

    Scenario: Top-level WHEN branch yields every row of its MATCH (indexes :P(n))
        Given an empty graph
        And with new index :P(n)
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN MATCH (n:P) RETURN n.n AS x ELSE RETURN 0 AS x
            """
        Then the result should be:
            | x |
            | 1 |
            | 2 |
            | 4 |

    Scenario: Top-level WHEN branch that yields no rows does not fall through to ELSE (indexes :P(n))
        Given an empty graph
        And with new index :P(n)
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a   | true |
            | ten | 10   |
        When executing query:
            """
            WHEN $a THEN MATCH (n:P) WHERE n.n > $ten RETURN n.n AS x ELSE RETURN 0 AS x
            """
        Then the result should be empty

    Scenario: Top-level WHEN aggregate in an untaken branch gives no row
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | b | false |
        When executing query:
            """
            WHEN $b THEN MATCH (n:P) RETURN count(n) AS x
            """
        Then the result should be empty

    Scenario: Top-level WHEN branch returns path, list and map columns with RETURN star
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN MATCH p = (q:Q)-[:R]->(m) UNWIND [1, 2] AS i WITH *, [i] AS l, {i: i} AS mp RETURN *
            """
        Then the result should be:
            | i | l   | m           | mp     | p                               | q           |
            | 1 | [1] | (:P {n: 4}) | {i: 1} | <(:Q {n: 3})-[:R]->(:P {n: 4})> | (:Q {n: 3}) |
            | 2 | [2] | (:P {n: 4}) | {i: 2} | <(:Q {n: 3})-[:R]->(:P {n: 4})> | (:Q {n: 3}) |

    Scenario: Top-level WHEN branches match columns by name, not by position
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | b | false |
        When executing query:
            """
            WHEN $b THEN RETURN 1 AS x, 2 AS y ELSE RETURN 20 AS y, 10 AS x
            """
        Then the result should be:
            | x  | y  |
            | 10 | 20 |

    Scenario: Top-level WHEN branches may return different types in one column
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | b | false |
        When executing query:
            """
            WHEN $b THEN RETURN 1 AS x ELSE RETURN 'a' AS x
            """
        Then the result should be:
            | x   |
            | 'a' |

    Scenario: Top-level WHEN write in the taken branch is visible later in that branch
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN CREATE (:T) WITH 1 AS one MATCH (t:T) RETURN count(t) AS x ELSE RETURN 0 AS x
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: Top-level WHEN untaken branch does not write
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | b | false |
        When executing query:
            """
            WHEN $b THEN CREATE (:T) RETURN 1 AS x ELSE MATCH (t:T) RETURN count(t) AS x
            """
        Then the result should be:
            | x |
            | 0 |

    Scenario: Top-level unit WHEN writes only the taken branch
        Given an empty graph
        And having executed:
            """
            WHEN false THEN CREATE (:T) ELSE CREATE (:U)
            """
        When executing query:
            """
            MATCH (n) RETURN labels(n) AS l
            """
        Then the result should be:
            | l     |
            | ['U'] |

    Scenario: Top-level unit WHEN without ELSE and no true predicate writes nothing
        Given an empty graph
        And having executed:
            """
            WHEN false THEN CREATE (:T)
            """
        When executing query:
            """
            MATCH (n) RETURN count(n) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: Top-level WHEN taken branch that writes and returns
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN MATCH (n:P) SET n.k = n.n * 10 RETURN sum(n.k) AS x ELSE RETURN 0 AS x
            """
        Then the result should be:
            | x  |
            | 70 |

    Scenario: Top-level WHEN branch must alias a returned property (control of: Top-level WHEN branch yields every row of its MATCH)
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN MATCH (n:Q) RETURN n.n
            """
        # Neo4j: Neo.ClientError.Statement.SyntaxError. memgraph's text: assert in a unit test.
        Then an error should be raised

    Scenario: Top-level WHEN predicate with an EXISTS subquery that holds
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        When executing query:
            """
            WHEN EXISTS { MATCH (:Q)-[:R]->(:P) } THEN RETURN 'yes' AS x ELSE RETURN 'no' AS x
            """
        Then the result should be:
            | x     |
            | 'yes' |

    Scenario: Top-level WHEN predicate with a COUNT subquery
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | two | 2 |
        When executing query:
            """
            WHEN COUNT { MATCH (n:P) } > $two THEN RETURN 'many' AS x ELSE RETURN 'few' AS x
            """
        Then the result should be:
            | x      |
            | 'many' |

    Scenario: Top-level WHEN predicate with a pattern
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        When executing query:
            """
            WHEN (:Q)-[:R]->() THEN RETURN 'yes' AS x ELSE RETURN 'no' AS x
            """
        Then the result should be:
            | x     |
            | 'yes' |

    Scenario: Top-level WHEN does not evaluate a predicate after the taken branch
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a   | true |
            | one | 1    |
        When executing query:
            """
            WHEN $a THEN RETURN 1 AS x WHEN 1 / ($one - 1) = 1 THEN RETURN 2 AS x
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: Top-level WHEN does not evaluate a subquery predicate after the taken branch
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
            | z | 0    |
        When executing query:
            """
            WHEN $a THEN RETURN 1 AS x WHEN EXISTS { UNWIND [1 / $z] AS u RETURN u } THEN RETURN 2 AS x ELSE RETURN 3 AS x
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: Top-level WHEN does not run an untaken branch
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a   | true |
            | one | 1    |
        When executing query:
            """
            WHEN $a THEN RETURN 1 AS x ELSE RETURN 1 / ($one - 1) AS x
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: Top-level WHEN predicate error is raised (control of the two laziness scenarios before it)
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | one | 1 |
        When executing query:
            """
            WHEN 1 / ($one - 1) = 1 THEN RETURN 1 AS x ELSE RETURN 2 AS x
            """
        # Neo4j: Neo.ClientError.Statement.ArithmeticError. memgraph's text: assert in a unit test.
        Then an error should be raised

    Scenario: Top-level WHEN with a nested braced WHEN
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true  |
            | b | false |
        When executing query:
            """
            WHEN $a THEN { WHEN $b THEN RETURN 1 AS x ELSE RETURN 2 AS x } ELSE RETURN 3 AS x
            """
        Then the result should be:
            | x |
            | 2 |

    Scenario: Top-level WHEN branch with a braced UNION ALL
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN { RETURN 1 AS x UNION ALL RETURN 2 AS x } ELSE RETURN 3 AS x
            """
        Then the result should be:
            | x |
            | 1 |
            | 2 |

    Scenario: Top-level WHEN branch with a conditional CALL
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true  |
            | b | false |
        When executing query:
            """
            WHEN $a THEN CALL () { WHEN $b THEN RETURN 1 AS y ELSE RETURN 2 AS y } RETURN y AS x
            """
        Then the result should be:
            | x |
            | 2 |

    Scenario: Top-level WHEN branch with a conditional EXISTS
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true  |
            | b | false |
        When executing query:
            """
            WHEN $a THEN RETURN EXISTS { WHEN $b THEN RETURN 1 AS y } AS x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: CASE WHEN at the top level is unchanged
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            RETURN CASE WHEN $a THEN 1 ELSE 2 END AS x
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: when stays usable as a variable name
        Given an empty graph
        And having executed:
            """
            CREATE (:P {n: 1}), (:P {n: 2}), (:Q {n: 3})-[:R]->(:P {n: 4})
            """
        When executing query:
            """
            WITH 1 AS when RETURN when AS x
            """
        Then the result should be:
            | x |
            | 1 |

    Scenario: A memory limit after a top-level WHEN applies to its branches
        # Deliberate divergence from Neo4j: memgraph-only clause on the outer query.
        # Without the limit the branch returns 1000000.
        Given an empty graph
        And parameters are:
            | a | true |
        When executing query:
            """
            WHEN $a THEN UNWIND range(1, 1000000) AS i WITH collect(i) AS l RETURN size(l) AS x QUERY MEMORY LIMIT 1 MB
            """
        Then an error should be raised

    Scenario: USING HOPS LIMIT before a top-level WHEN is query-wide
        # Deliberate divergence from Neo4j: memgraph-only directive; the hop budget is shared by the whole query, as for UNION legs
        Given an empty graph
        And having executed:
            """
            CREATE (:A)-[:R]->(:A)-[:R]->(:A)
            """
        And parameters are:
            | a | true |
        When executing query:
            """
            USING HOPS LIMIT 1 WHEN $a THEN MATCH (a:A)-[*]->(b) RETURN count(*) AS x ELSE RETURN 0 AS x
            """
        Then the result should be:
            | x |
            | 1 |
