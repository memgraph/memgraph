Feature: Memgraph only tests (queries in which we choose to be incompatible with neo4j)

    Scenario: Multiple sets (undefined behaviour)
        Given an empty graph
        And having executed
            """
            CREATE (n{x: 3})-[:X]->(m{x: 5})
            """
        When executing query:
            """
            MATCH (n)--(m) SET n.x = n.x + 1 SET m.x = m.x + 2 SET m.x = n.x RETURN n.x
            """
	    # TODO: Figure out if we can define a test with multiple possible outputs in cucumber,
	    # until then this test just documents behaviour instead of testing it.
            #        Then the result should be:
            #            | n.x |    | n.x |
            #            |  5  | or |  7  |
            #            |  5  |    |  7  |

    Scenario: Multiple comparisons
        Given an empty graph
        When executing query:
            """
            RETURN 1 < 10 > 5 < 7 > 6 < 8 AS x
            """
        Then the result should be:
            | x    |
            | true |

    Scenario: Use deleted node
        Given an empty graph
        When executing query:
            """
            CREATE(a:A), (b:B), (c:C), (a)-[:T]->(b) WITH a DETACH DELETE a WITH a MATCH(a)-[r:T]->() RETURN r
            """
        Then an error should be raised

    Scenario: In test3
        When executing query:
            """
            WITH [[1], 2, 3, 4] AS l
            RETURN 1 IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: In test8
        When executing query:
            """
            WITH [[[[1]]], 2, 3, 4] AS l
            RETURN 1 IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: Keyword as symbolic name
        Given an empty graph
        And having executed
            """
            CREATE(a:DELete)
            """
        When executing query:
            """
            MATCH (n) RETURN n
            """
        Then the result should be:
            | n         |
            | (:DELete) |

    Scenario: Aggregation in CASE with constant arms:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN CASE WHEN true THEN count(n) ELSE 0 END AS c
            """
        Then the result should be:
            | c |
            | 3 |

    Scenario: Aggregation as the test of a simple CASE:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN CASE count(n) WHEN 3 THEN 'three' WHEN 2 THEN 'two' ELSE 'other' END AS c
            """
        Then the result should be:
            | c       |
            | 'three' |

    Scenario: Aggregation in every arm of a simple CASE on an aggregation, beside a grouping key:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN n.age > 15 AS old, CASE count(n) WHEN 1 THEN min(n.name) WHEN 2 THEN max(n.name) ELSE 'many' END AS c, CASE WHEN sum(n.age) > 30 THEN 'big' ELSE 'small' END AS s
            """
        Then the result should be:
            | old   | c   | s       |
            | false | 'a' | 'small' |
            | true  | 'c' | 'big'   |

    # The query above is the one shape a scenario here cannot cover under USING PARALLEL EXECUTION, which is how it
    # reaches the planner with its CASE test node still shared rather than cloned by the parse cache. Disk storage
    # refuses the directive outright, so the shared node is pinned by
    # TestPlanner.MatchReturnSimpleCaseOnAggregationSharingOneTest instead.

    Scenario: Aggregation in an inner arm of a searched CASE with several arms:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN CASE WHEN n.age < 15 THEN 'young' WHEN count(n) > 1 THEN 'many' ELSE 'one' END AS c
            """
        Then the result should be:
            | c       |
            | 'young' |
            | 'many'  |

    Scenario: Aggregation in CASE over empty input with a list arm:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Nope) RETURN CASE WHEN true THEN count(n) ELSE [] END AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: Aggregation in CASE with a condition correlated through an EXISTS body:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN CASE WHEN EXISTS { MATCH (:Person) WHERE n.age > 15 } THEN count(n) ELSE -1 END AS c
            """
        Then the result should be:
            | c  |
            | -1 |
            | 2  |

    Scenario: Aggregation in CASE beside an uncorrelated EXISTS over empty input
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Nope) RETURN CASE WHEN EXISTS { MATCH (:Person) } THEN count(n) ELSE -1 END AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: Aggregation in CASE beside an uncorrelated comprehension over empty input
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Nope) RETURN CASE WHEN size([(:Person)-[]-() | 1]) >= 0 THEN count(n) ELSE -1 END AS c
            """
        Then the result should be:
            | c |
            | 0 |

    # An arm that does not aggregate becomes a grouping key, so it is evaluated for every input row and not only for
    # the rows whose condition selects it. The ELSE here divides by the zero its WHEN excludes, and still throws.
    Scenario: An arm of a CASE that aggregates is evaluated for every row
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', d: 0}), (:Person {name: 'b', d: 2})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN CASE WHEN n.d <> 0 THEN count(n) ELSE 10 / n.d END AS c
            """
        Then an error should be raised
