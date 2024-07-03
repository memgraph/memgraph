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

    Scenario: Aggregation in CASE:
        Given an empty graph
        When executing query:
            """
            MATCH (n) RETURN CASE count(n) WHEN 10 THEN 10 END
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
            | Enum Name | Enum Values     |
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
            | Enum Name | Enum Values     |
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
            | false  |

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
        Then an error should be raised

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
