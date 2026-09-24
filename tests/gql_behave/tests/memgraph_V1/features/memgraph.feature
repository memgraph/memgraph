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

    Scenario: Aggregation in every arm of a simple CASE on an aggregation, without the parse cache:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            USING PARALLEL EXECUTION 2 MATCH (n:Person) RETURN n.age > 15 AS old, CASE count(n) WHEN 1 THEN min(n.name) WHEN 2 THEN max(n.name) ELSE 'many' END AS c, CASE WHEN sum(n.age) > 30 THEN 'big' ELSE 'small' END AS s
            """
        Then the result should be:
            | old   | c   | s       |
            | false | 'a' | 'small' |
            | true  | 'c' | 'big'   |

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

    # Issue #4917: the expected result here is the defect, not the intent. An uncorrelated branch is a grouping key
    # only because it is planned below the Aggregate, so an empty input leaves no row to carry it and the one row
    # the query should return is lost. This scenario and the next one change when #4917 is fixed.
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
        Then the result should be empty

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
        Then the result should be empty

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

    Scenario: Create enum:
        Given an empty graph
        When executing query:
            """
            CREATE ENUM Status VALUES { Good, Bad };
            """
        Then the result should be empty

    Scenario: Show enums:
        Given an empty graph
        # Values will be used from the previous scenario
        When executing query:
            """
            SHOW ENUMS;
            """
        Then the result should be:
            | Enum Name | Enum Values     |
            | 'Status'  | ['Good', 'Bad'] |

    Scenario: Add value to enum:
        Given an empty graph
        And having executed
            """
            ALTER ENUM Status ADD VALUE Medium;
            """
        When executing query:
            """
            SHOW ENUMS;
            """
        Then the result should be:
            | Enum Name | Enum Values               |
            | 'Status'  | ['Good', 'Bad', 'Medium'] |

    Scenario: Update value in enum:
        Given an empty graph
        And having executed
            """
            ALTER ENUM Status UPDATE VALUE Medium TO Average;
            """
        When executing query:
            """
            SHOW ENUMS;
            """
        Then the result should be:
            | Enum Name | Enum Values                |
            | 'Status'  | ['Good', 'Bad', 'Average'] |

    Scenario: Compare enum values for equality:
        Given an empty graph
        # Values will be used from the previous scenario
        When executing query:
            """
            RETURN Status::Good = Status::Good AS result1, Status::Good = Status::Bad AS result2
            """
        Then the result should be:
            | result1 | result2 |
            | true    | false   |

    Scenario: Compare different enums for equality:
        Given an empty graph
        # Values will be used from the previous scenario
        And having executed
            """
            CREATE ENUM NewEnum VALUES { Good, Bad };
            """
        When executing query:
            """
            RETURN Status::Good = NewEnum::Good AS result1
            """
        Then the result should be:
            | result1 |
            | false   |

    Scenario: Create an edge with an enum property:
        Given an empty graph
        When executing query:
            """
            CREATE (n:Person {s: Status::Good})-[:KNOWS {s: Status::Bad}]->(m:Person {s: Status::Bad})
            """
        Then the result should be empty

    Scenario: Get nodes and edges with enum properties:
        Given an empty graph
        And having executed
            """
            CREATE (n:Person {s: Status::Good})-[:KNOWS {s: Status::Bad}]->(m:Person {s: Status::Bad})
            """
        When executing query:
            """
            MATCH (n)-[e]->(m) RETURN n, n.s, e, e.s, m
            """
        Then the result should be:
            | n                                                          | n.s                                       | e                                                        | e.s                                      | m                                                         |
            | (:Person{s:{'__type':'mg_enum','__value':'Status::Good'}}) | {__type:'mg_enum',__value:'Status::Good'} | [:KNOWS{s:{'__type':'mg_enum','__value':'Status::Bad'}}] | {__type:'mg_enum',__value:'Status::Bad'} | (:Person{s:{'__type':'mg_enum','__value':'Status::Bad'}}) |

    Scenario: Filter nodes by enum property equal op:
        Given an empty graph
        And having executed
            """
            CREATE (n:Person {s: Status::Good})-[:KNOWS {s: Status::Bad}]->(m:Person {s: Status::Bad})
            """
        When executing query:
            """
            MATCH (n) WHERE n.s = Status::Bad RETURN n
            """
        Then the result should be:
            | n                                                         |
            | (:Person{s:{'__type':'mg_enum','__value':'Status::Bad'}}) |

    Scenario: Filter nodes by enum property comparison op:
        Given an empty graph
        And having executed
            """
            CREATE (n:Person {s: Status::Good})-[:KNOWS {s: Status::Bad}]->(m:Person {s: Status::Bad})
            """
        When executing query:
            """
            MATCH (n) WHERE n.s <= Status::Bad RETURN n
            """
        Then the result should be empty

    Scenario: Compare enum values for ordering:
        Given an empty graph
        # Values will be used from the previous scenario
        When executing query:
            """
            RETURN Status::Good <= Status::Bad AS result1, Status::Good > Status::Good AS result2
            """
        Then the result should be:
            | result1 | result2 |
            | null    | null    |

    Scenario: Enum equal to a comparison bound answers null:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {s: Status::Good}), (:Person {s: Status::Bad})
            """
        When executing query:
            """
            MATCH (n) RETURN n.s = Status::Bad AS eq, n.s <= Status::Bad AS le ORDER BY eq
            """
        Then the result should be:
            | eq    | le   |
            | false | null |
            | true  | null |

    Scenario: Compare enum values for inequality:
        Given an empty graph
        # Values will be used from the previous scenario
        When executing query:
            """
            RETURN Status::Good != Status::Good AS result1, Status::Good != Status::Bad AS result2
            """
        Then the result should be:
            | result1 | result2 |
            | false   | true    |

    Scenario: Alter enum remove value:
        Given an empty graph
        When executing query:
            """
            ALTER ENUM Status REMOVE VALUE Good;
            """
        Then an error should be raised

    Scenario: Drop enum:
        Given an empty graph
        When executing query:
            """
            DROP ENUM Status;
            """
        Then an error should be raised

    Scenario: EXPLAIN tolerates leading whitespace
        Given an empty graph
        When executing query:
            """

                EXPLAIN RETURN 1
            """
        Then the result should be:
            | QUERY PLAN       |
            | ' * Produce {0}' |
            | ' * Once'        |

    Scenario: PROFILE tolerates leading whitespace
        Given an empty graph
        When executing query:
            """

                PROFILE RETURN 1
            """
        When executing query:
            """
            RETURN 1 AS n
            """
        Then the result should be:
            | n |
            | 1 |
