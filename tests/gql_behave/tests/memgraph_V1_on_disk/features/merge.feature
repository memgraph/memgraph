Feature: Merge feature

    Scenario: Merge node test01
        Given an empty graph
        And having executed:
            """
            CREATE (:X{a: 1})
            """
        And having executed:
            """
            MERGE(n:X)
            """
        When executing query:
            """
            MATCH(n) RETURN n
            """
        Then the result should be:
            | n          |
            | (:X{a: 1}) |

    Scenario: Merge node test02
        Given an empty graph
        And having executed:
            """
            CREATE (:X)
            """
        And having executed:
            """
            MERGE(n:X:Y)
            """
        When executing query:
            """
            MATCH(n) RETURN n
            """
        Then the result should be:
            | n      |
            | (:X)   |
            | (:X:Y) |

    Scenario: Merge node test03
        Given an empty graph
        And having executed:
            """
            CREATE (:Y{a: 1})
            """
        And having executed:
            """
            MERGE(n:X)
            """
        When executing query:
            """
            MATCH(n) RETURN n
            """
        Then the result should be:
            | n          |
            | (:X)       |
            | (:Y{a: 1}) |

    Scenario: Merge node test04
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1, b: 2})
            """
        And having executed:
            """
            MERGE(n{a: 1, b: 2})
            """
        When executing query:
            """
            MATCH(n) RETURN n
            """
        Then the result should be:
            | n              |
            | ({a: 1, b: 2}) |

    Scenario: Merge node test05
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 2})
            """
        And having executed:
            """
            MERGE(n{a: 1, b:2})
            """
        When executing query:
            """
            MATCH(n) RETURN n
            """
        Then the result should be:
            | n              |
            | ({a: 2})       |
            | ({a: 1, b: 2}) |

    Scenario: Merge node test06
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 2})
            """
        And having executed:
            """
            MERGE(n{a: 2, b: 2})
            """
        When executing query:
            """
            MATCH(n) RETURN n
            """
        Then the result should be:
            | n              |
            | ({a: 2})       |
            | ({a: 2, b: 2}) |

    Scenario: Merge node test07
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 2})
            """
        And having executed:
            """
            MERGE(n:A{a: 2})
            """
        When executing query:
            """
            MATCH(n) RETURN n
            """
        Then the result should be:
            | n          |
            | (:A{a: 2}) |
            | ({a: 2})   |

    Scenario: Merge node test08
        Given an empty graph
        And having executed:
            """
            CREATE (:A:B{a: 2, b: 1})
            """
        And having executed:
            """
            MERGE(n:A{a: 2})
            """
        When executing query:
            """
            MATCH(n) RETURN n
            """
        Then the result should be:
            | n                  |
            | (:A:B{a: 2, b: 1}) |

    Scenario: Merge node test09
        Given an empty graph
        And having executed:
            """
            CREATE (:A{a: 2}), (:A{a: 2}), (:A{a: 1})
            """
        And having executed:
            """
            MATCH(n:A)
            MERGE(m:B{x: n.a})
            """
        When executing query:
            """
            MATCH(n:B) RETURN *
            """
        Then the result should be:
            | n          |
            | (:B{x: 1}) |
            | (:B{x: 2}) |

    Scenario: Merge relationship test01
        Given an empty graph
        And having executed:
            """
            CREATE (a), (b), (a)-[:X]->(b)
            """
        And having executed:
            """
            MATCH (a)--(b)
            MERGE ((a)-[r:X]-(b))
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN *
            """
        Then the result should be:
            | r    |
            | [:X] |

    Scenario: Merge relationship test02
        Given an empty graph
        And having executed:
            """
            CREATE (a), (b), (a)-[:X]->(b)
            """
        And having executed:
            """
            MATCH (a)--(b)
            MERGE ((a)-[r:Y]-(b))
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN *
            """
        Then the result should be:
            | r    |
            | [:X] |
            | [:Y] |

    Scenario: Merge relationship test03
        Given an empty graph
        And having executed:
            """
            CREATE (a), (b), (a)-[:X{a: 1}]->(b)
            """
        And having executed:
            """
            MATCH (a)--(b)
            MERGE ((a)-[r:X{a: 1}]-(b))
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN *
            """
        Then the result should be:
            | r          |
            | [:X{a: 1}] |

    Scenario: Merge relationship test04
        Given an empty graph
        And having executed:
            """
            CREATE (a), (b), (a)-[:X{a: 1}]->(b)
            """
        And having executed:
            """
            MATCH (a)--(b)
            MERGE ((a)-[r:X{a: 2}]-(b))
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN *
            """
        Then the result should be:
            | r          |
            | [:X{a: 1}] |
            | [:X{a: 2}] |

    Scenario: Merge relationship test05
        Given an empty graph
        And having executed:
            """
            CREATE (a), (b), (a)-[:X{a: 1}]->(b)
            """
        And having executed:
            """
            MATCH (a)--(b)
            MERGE ((a)-[r:Y{a: 1}]-(b))
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN *
            """
        Then the result should be:
            | r          |
            | [:X{a: 1}] |
            | [:Y{a: 1}] |

    Scenario: Merge relationship test06
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{a: 1}), (b:B{b: 1}), (c:C), (a)-[:X]->(c), (c)<-[:Y]-(b)
            """
        And having executed:
            """
            MERGE (:A)-[:X]->(:C)<-[:Y]-(:B)
            """
        When executing query:
            """
            MATCH (a)-[r]->(b) RETURN *
            """
        Then the result should be:
            | a          | r    | b    |
            | (:A{a: 1}) | [:X] | (:C) |
            | (:B{b: 1}) | [:Y] | (:C) |

    Scenario: Merge relationship test07
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{a: 1}), (b:B{b: 1}), (c:C), (a)-[:X{x: 1}]->(c), (c)<-[:Y{y: 1}]-(b)
            """
        And having executed:
            """
            MERGE (:A)-[:X]->(:D)<-[:Y]-(:B)
            """
        When executing query:
            """
            MATCH (a)-[r]->(b) RETURN *
            """
        Then the result should be:
            | a          | r          | b    |
            | (:A)       | [:X]       | (:D) |
            | (:B)       | [:Y]       | (:D) |
            | (:A{a: 1}) | [:X{x: 1}] | (:C) |
            | (:B{b: 1}) | [:Y{y: 1}] | (:C) |

    Scenario: Merge relationship test08
        Given an empty graph
        And having executed:
            """
            CREATE (a:A:X), (b:B:Y)
            """
        And having executed:
            """
            MATCH (a:A), (b:B)
            MERGE (a)-[:R]-(b)
            """
        When executing query:
            """
            MATCH (a)-[r]-(b) RETURN *
            """
        Then the result should be:
            | a      | r    | b      |
            | (:A:X) | [:R] | (:B:Y) |
            | (:B:Y) | [:R] | (:A:X) |

    Scenario: Merge relationship test09
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{a: 1}), (b:B{a: 2}), (c:C{a: 2})
            """
        When executing query:
            """
            MATCH (n)
            MERGE (m:X{a: n.a})
            MERGE (n)-[r:R]->(m)
            RETURN *
            """
        Then the result should be:
            | n          | r    | m          |
            | (:A{a: 1}) | [:R] | (:X{a: 1}) |
            | (:B{a: 2}) | [:R] | (:X{a: 2}) |
            | (:C{a: 2}) | [:R] | (:X{a: 2}) |

    Scenario: Merge relationship test10
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{a: 1}), (b:B{a: 2}), (c:C{a: 2})
            """
        When executing query:
            """
            MATCH (n)
            MERGE (n)-[r:R]->(m:X{a: n.a})
            RETURN *
            """
        Then the result should be:
            | n          | r    | m          |
            | (:A{a: 1}) | [:R] | (:X{a: 1}) |
            | (:B{a: 2}) | [:R] | (:X{a: 2}) |
            | (:C{a: 2}) | [:R] | (:X{a: 2}) |

    Scenario: Merge OnCreate test01
        Given an empty graph
        When executing query:
            """
            MERGE (a:X)
            ON CREATE SET a.a = 1
            RETURN *
            """
        Then the result should be:
            | a          |
            | (:X{a: 1}) |

    Scenario: Merge OnMatch test01
        Given an empty graph
        And having executed:
            """
            CREATE(:A), (:A), (:B)
            """
        And having executed:
            """
            MERGE (a:A)
            ON MATCH SET a.a = 1
            """
        When executing query:
            """
            MATCH (n) RETURN *
            """
        Then the result should be:
            | n          |
            | (:A{a: 1}) |
            | (:A{a: 1}) |
            | (:B)       |

    Scenario: Merge with Unwind test01
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1})
            """
        And having executed:
            """
            UNWIND [1, 2, 3] AS a
            MERGE({a: a})
            """
        When executing query:
            """
            MATCH (n) RETURN *
            """
        Then the result should be:
            | n        |
            | ({a: 1}) |
            | ({a: 2}) |
            | ({a: 3)) |

    Scenario: Merge node with null property error
        Given an empty graph
        When executing query:
            """
            MERGE ({id: null})
            """
        Then an error should be raised

    Scenario: Merge node with one null property error
        Given an empty graph
        When executing query:
            """
            MERGE ({id2: 1, id: null})
            """
        Then an error should be raised

    Scenario: Merge edge with null property error
        Given an empty graph
        When executing query:
            """
            MERGE ()-[:TYPE {id:null}]->()
            """
        Then an error should be raised

    Scenario: Merge node with unwind with null property error
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, null, 3] as x
            MERGE ({id: x})
            """
        Then an error should be raised

    Scenario: Merge edge with unwind with null property error
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, null, 3] as x
            MERGE ()-[:TYPE {id:x}]->()
            """
        Then an error should be raised

    Scenario: Merge node with null in properties on match passes
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1})
            """
        When executing query:
            """
            MERGE (n {a: 1})
            ON MATCH SET n.prop = null
            ON CREATE SET n.prop = null;
            """
        Then the result should be empty

    Scenario: Merge edge with null in properties on match passes
        Given an empty graph
        And having executed:
            """
            CREATE ()-[:TYPE {a: 1}]->()
            """
        When executing query:
            """
            MERGE ()-[r:TYPE {a: 1}]->()
            ON MATCH SET r.prop = null
            ON CREATE SET r.prop = null;
            """
        Then the result should be empty

    Scenario: Merge node with null in properties on create passes
        Given an empty graph
        When executing query:
            """
            MERGE (n {a: 1})
            ON MATCH SET n.prop = null
            ON CREATE SET n.prop = null;
            """
        Then the result should be empty

    Scenario: Merge edge with null in properties on create passes
        Given an empty graph
        When executing query:
            """
            MERGE ()-[r:TYPE {a: 1}]->()
            ON MATCH SET r.prop = null
            ON CREATE SET r.prop = null;
            """
        Then the result should be empty
