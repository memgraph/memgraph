Feature: Patterns

    Scenario: Optional match with expressions
        Given an empty graph
        And having executed:
            """
            CREATE ()
            """
        When executing query:
            """
            MATCH (a)
            OPTIONAL MATCH (a)-[:NEXT]->(b)
            WITH a, b, (b IS NOT NULL) AS has_b
            return a, b, has_b
            """
        Then the result should be:
            | a  | b    | has_b |
            | () | null | false |

    Scenario: Positive pattern expression
        Given an empty graph
        And having executed:
            """
            CREATE ()-[:NEXT]->()
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->()
            RETURN a
            """
        Then the result should be:
            | a  |
            | () |


    Scenario: Pattern expression with negation
        Given an empty graph
        And having executed:
            """
            CREATE ()
            """
        When executing query:
            """
            MATCH (a)
            WHERE NOT (a)-[:NEXT]->()
            RETURN a
            """
        Then the result should be:
            | a  |
            | () |

    Scenario: Pattern expression 1-hop error by introducing new variable
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->(b)
            RETURN a
            """
        Then an error should be raised


    Scenario: Pattern expression 1-hop negation error by introducing new variable
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE NOT (a)-[:NEXT]->(b)
            RETURN a
            """
        Then an error should be raised

    Scenario: Pattern expression 1-hop error by introducing new variable in relationship
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[r:NEXT]->()
            RETURN a
            """
        Then an error should be raised


    Scenario: Pattern expression 1-hop negation error by introducing new variable
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE NOT (a)-[r:NEXT]->()
            RETURN a
            """
        Then an error should be raised

    Scenario: Pattern expression 1-hop returning node
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->()
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 1}) |

    Scenario: Pattern expression 1-hop returning empty because of different edge type
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:TYPE]->()
            RETURN a
            """
        Then the result should be empty

    Scenario: Pattern expression 1-hop returning empty because of different label
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->(:Node)
            RETURN a
            """
        Then the result should be empty

    Scenario: Pattern expression 1-hop returning node with correct label
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->(:Node {id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->(:Node)
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 1}) |

    Scenario: Pattern expression 1-hop negation returning node
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE NOT (a)-[:NEXT]->()
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 2}) |


    Scenario: Pattern expression 1-hop with AND
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->() AND NOT (a)-[:NEXT]->()
            RETURN a
            """
        Then the result should be empty

    Scenario: Pattern expression 1-hop with OR
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->() OR NOT (a)-[:NEXT]->()
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 1}) |
            | ({id: 2}) |

    Scenario: Pattern expression 1-hop with OR reverse arrow
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->() OR (a)<-[:NEXT]->()
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 1}) |
            | ({id: 2}) |

    Scenario: Pattern expression 2-hop empty
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->()-[:NEXT]->()
            RETURN a
            """
        Then the result should be empty

    Scenario: Pattern expression 2-hop returns node
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})-[:NEXT]->({id: 3})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->()-[:NEXT]->()
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 1}) |

    Scenario: Pattern expression 2-hop returns final node
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})-[:NEXT]->({id: 3})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)<-[:NEXT]-()<-[:NEXT]-()
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 3}) |

    Scenario: Pattern expression 2-hop negation returns other nodes
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})-[:NEXT]->({id: 3})
            """
        When executing query:
            """
            MATCH (a)
            WHERE NOT (a)-[:NEXT]->()-[:NEXT]->()
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 2}) |
            | ({id: 3}) |

    Scenario: Pattern expression 2-hop OR
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})-[:NEXT]->({id: 3})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->({id: 3}) OR (a)-[:NEXT]->({id: 2})
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 1}) |
            | ({id: 2}) |

    Scenario: Pattern expression 2-hop AND between
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 1})-[:NEXT]->({id: 2})-[:NEXT]->({id: 3})
            """
        When executing query:
            """
            MATCH (a)
            WHERE (a)-[:NEXT]->({id: 3}) AND (a)<-[:NEXT]-({id: 1})
            RETURN a
            """
        Then the result should be:
            | a         |
            | ({id: 2}) |

    Scenario: Pattern expression with only node returns error
        Given an empty graph
        And having executed:
            """
            CREATE (n)
            """
        When executing query:
            """
            MATCH (n)
            WHERE (n)
            RETURN n
            """
        Then an error should be raised

    Scenario: Pattern expression negation with only node returns error
        Given an empty graph
        And having executed:
            """
            CREATE (n)
            """
        When executing query:
            """
            MATCH (n)
            WHERE NOT (n)
            RETURN n
            """
        Then an error should be raised
