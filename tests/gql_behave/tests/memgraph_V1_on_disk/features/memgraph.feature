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

    Scenario: Aggregation in the condition of a CASE:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN CASE WHEN count(n) > 2 THEN 'many' ELSE 'few' END AS c
            """
        Then the result should be:
            | c      |
            | 'many' |

    Scenario: Aggregation in both arms of a CASE:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN CASE WHEN count(n) > 100 THEN count(n) ELSE sum(n.age) END AS c
            """
        Then the result should be:
            | c  |
            | 50 |

    Scenario: Aggregation in CASE over empty input:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Nope) RETURN CASE WHEN true THEN count(n) ELSE 0 END AS c
            """
        Then the result should be:
            | c |
            | 0 |

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

    Scenario: Aggregation in CASE over empty input with a computed constant arm:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Nope) RETURN CASE WHEN true THEN count(n) ELSE 1 + 1 END AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: Aggregation in CASE without a match:
        Given an empty graph
        When executing query:
            """
            RETURN CASE WHEN true THEN count(*) ELSE 0 END AS c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: Aggregation in CASE beside an explicit grouping key:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) RETURN n.age AS age, CASE WHEN n.age > 15 THEN count(n) ELSE 0 END AS c
            """
        Then the result should be:
            | age | c |
            | 10  | 0 |
            | 20  | 2 |

    Scenario: Aggregation in CASE with a null grouping key:
        Given an empty graph
        When executing query:
            """
            UNWIND [1, null, 2, null] AS x RETURN x AS k, CASE WHEN x IS NULL THEN count(*) ELSE -count(*) END AS c
            """
        Then the result should be:
            | k    | c  |
            | 1    | -1 |
            | 2    | -1 |
            | null | 2  |

    Scenario: Aggregation in CASE in a WITH, filtered afterwards:
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a', age: 10}), (:Person {name: 'b', age: 20}), (:Person {name: 'c', age: 20})
            """
        When executing query:
            """
            MATCH (n:Person) WITH CASE WHEN true THEN count(n) ELSE 0 END AS c WHERE c > 1 RETURN c
            """
        Then the result should be:
            | c |
            | 3 |
