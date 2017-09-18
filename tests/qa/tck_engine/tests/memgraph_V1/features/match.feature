Feature: Match

    Scenario: Create node and self relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (n)-[:X]->(n)<-[:Y]-(n)
            """
        When executing query:
            """
            MATCH ()-[a]-()-[b]-() RETURN a, b
            """
        Then the result should be:
            |   a    |    b    |
            |  [:X]  |   [:Y]  |
            |  [:Y]  |   [:X]  |

    Scenario: Create node and self relationship and match
        Given an empty graph
        And having executed:
            """
            CREATE (n)-[:X]->(n)
            """
        When executing query:
            """
            MATCH ()-[a]-() RETURN a
            """
        Then the result should be:
            |   a    |
            |  [:X]  |

    Scenario: Create node and self relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (n)-[:X]->(n)<-[:Y]-(n)
            """
        When executing query:
            """
            MATCH ()<-[a]-()-[b]->() RETURN a, b
            """
        Then the result should be:
            |   a    |    b    |
            |  [:X]  |   [:Y]  |
            |  [:Y]  |   [:X]  |

    Scenario: Create multiple nodes and relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE ()-[:X]->()<-[:Y]-()
            """
        When executing query:
            """
            MATCH ()-[a]-()-[b]-() RETURN a, b
            """
        Then the result should be:
            |   a    |    b    |
            |  [:X]  |   [:Y]  |
            |  [:Y]  |   [:X]  |

    Scenario: Create multiple nodes and relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE ()-[:X]->()<-[:Y]-()
            """
        When executing query:
            """
            MATCH ()<-[a]-()-[b]-() RETURN a, b
            """
        Then the result should be empty

    Scenario: Create cycle and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X]->()-[:Y]->()-[:Z]->(a)
            """
        When executing query:
            """
            MATCH ()-[a]->()-[b]->() RETURN a, b
            """
        Then the result should be:
            |   a    |    b    |
            |  [:X]  |   [:Y]  |
            |  [:Y]  |   [:Z]  |
            |  [:Z]  |   [:X]  |

    Scenario: Create cycle and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X]->()-[:Y]->()-[:Z]->(a)
            """
        When executing query:
            """
            MATCH ()-[a]-()-[b]-() RETURN a, b
            """
        Then the result should be:
            |   a    |    b    |
            |  [:X]  |   [:Y]  |
            |  [:Y]  |   [:Z]  |
            |  [:Z]  |   [:X]  |
            |  [:X]  |   [:Z]  |
            |  [:Y]  |   [:X]  |
            |  [:Z]  |   [:Y]  |

    Scenario: Create cycle and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X]->()-[:Y]->()-[:Z]->(a)
            """
        When executing query:
            """
            MATCH ()<-[a]-()-[b]->() RETURN a, b
            """
        Then the result should be empty

    Scenario: Create cycle and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X]->()-[:Y]->()-[:Z]->(a)
            """
        When executing query:
            """
            MATCH ()-[a]->()-[]->()-[]->()-[]->() RETURN a
            """
        Then the result should be empty

    Scenario: Create two nodes with three relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X]->(b)-[:Y]->(a)-[:Z]->(b)
            """
        When executing query:
            """
            MATCH ()-[a]->()-[b]->()-[c]->() RETURN a, b, c
            """
        Then the result should be:
            |   a    |    b    |    c    |
            |  [:X]  |   [:Y]  |   [:Z]  |
            |  [:Z]  |   [:Y]  |   [:X]  |

    Scenario: Create two nodes with three relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X]->(b)-[:Y]->(a)-[:Z]->(b)
            """
        When executing query:
            """
            MATCH ()-[a]-()-[b]-()-[c]-() RETURN a, b, c
            """
        Then the result should be:
            |   a    |    b    |    c    |
            |  [:X]  |   [:Y]  |   [:Z]  |
            |  [:X]  |   [:Z]  |   [:Y]  |
            |  [:Y]  |   [:X]  |   [:Z]  |
            |  [:Y]  |   [:Z]  |   [:X]  |
            |  [:Z]  |   [:Y]  |   [:X]  |
            |  [:Z]  |   [:X]  |   [:Y]  |
            |  [:X]  |   [:Y]  |   [:Z]  |
            |  [:X]  |   [:Z]  |   [:Y]  |
            |  [:Y]  |   [:X]  |   [:Z]  |
            |  [:Y]  |   [:Z]  |   [:X]  |
            |  [:Z]  |   [:Y]  |   [:X]  |
            |  [:Z]  |   [:X]  |   [:Y]  |

    Scenario: Create two nodes with three relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X{a: 1.0}]->(b)-[:Y]->(a)-[:Z]->(b)
            """
        When executing query:
            """
            MATCH ()-[a{a: 1.0}]-()-[b]-()-[c]-() RETURN a, b, c
            """
        Then the result should be:
            |   a            |    b    |    c    |
            |  [:X{a: 1.0}]  |   [:Y]  |   [:Z]  |
            |  [:X{a: 1.0}]  |   [:Z]  |   [:Y]  |
            |  [:X{a: 1.0}]  |   [:Y]  |   [:Z]  |
            |  [:X{a: 1.0}]  |   [:Z]  |   [:Y]  |

    Scenario: Create two nodes with three relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X{a: 1.0}]->(b)-[:Y]->(a)-[:Z]->(b)
            """
        When executing query:
            """
            MATCH ()-[a{a: 1.0}]-()-[b]-()-[c:Y]-() RETURN a, b, c
            """
        Then the result should be:
            |   a            |    b    |    c    |
            |  [:X{a: 1.0}]  |   [:Z]  |   [:Y]  |
            |  [:X{a: 1.0}]  |   [:Z]  |   [:Y]  |

    Scenario: Create two nodes with three relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X{a: 1.0}]->(b)-[:Y{a: 1.0}]->(a)-[:Z]->(b)
            """
        When executing query:
            """
            MATCH ()-[a{a: 1.0}]-()-[b]-()-[c:Y]-() RETURN a, b, c
            """
        Then the result should be:
            |   a            |    b    |    c            |
            |  [:X{a: 1.0}]  |   [:Z]  |   [:Y{a: 1.0}]  |
            |  [:X{a: 1.0}]  |   [:Z]  |   [:Y{a: 1.0}]  |

    Scenario: Create two nodes with three relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:X{a: 1.0}]->(b)-[:Y{a: 1.0}]->(a)-[:Z]->(b)
            """
        When executing query:
            """
            MATCH ()-[a{a: 1.0}]-()-[b]-()-[c{a: 1.0}]-() RETURN a, b, c, c.a as t
            """
        Then the result should be:
            |   a            |    b    |    c            |  t  |
            |  [:X{a: 1.0}]  |   [:Z]  |   [:Y{a: 1.0}]  | 1.0 |
            |  [:X{a: 1.0}]  |   [:Z]  |   [:Y{a: 1.0}]  | 1.0 |
            |  [:Y{a: 1.0}]  |   [:Z]  |   [:X{a: 1.0}]  | 1.0 |
            |  [:Y{a: 1.0}]  |   [:Z]  |   [:X{a: 1.0}]  | 1.0 |

    Scenario: Create two nodes with three relationships and match
        Given an empty graph
        And having executed:
            """
            CREATE (a:T{c: True})-[:X{x: 2.5}]->(:A:B)-[:Y]->()-[:Z{r: 1}]->(a)
            """
        When executing query:
            """
            MATCH (:T{c: True})-[a:X{x: 2.5}]->(node:A:B)-[:Y]->()-[:Z{r: 1}]->() RETURN a AS node, node AS a
            """
        Then the result should be:
            |   node         |   a      |
            |  [:X{x: 2.5}]  |  (:A:B)  |

    Scenario: Create and match with label
        Given graph "graph_01"
        When executing query:
            """
            MATCH (n:Person) RETURN n
            """
        Then the result should be:
            |              n                |
            |     (:Person {age: 20})       |
            |  (:Person :Student {age: 20}) |
            |     (:Person {age: 21})       |

    Scenario: Create and match with label
        Given graph "graph_01"
        When executing query:
            """
            MATCH (n:Student) RETURN n
            """
        Then the result should be:
            |              n                |
            |  (:Person :Student {age: 20}) |
            |      (:Student {age: 21})     |

    Scenario: Create, match with label and property
        Given graph "graph_01"
        When executing query:
            """
            MATCH (n:Person {age: 20}) RETURN n AS x
            """
        Then the result should be:
            |              x                |
            |     (:Person {age: 20})       |
            |  (:Person :Student {age: 20}) |

    Scenario: Create, match with label and filter property using WHERE
        Given graph "graph_01"
        When executing query:
            """
            MATCH (n:Person) WHERE n.age = 20 RETURN n
            """
        Then the result should be:
            |              n                |
            |     (:Person {age: 20})       |
            |  (:Person :Student {age: 20}) |

    Scenario: Create and match with property
        Given graph "graph_01"
        When executing query:
            """
            MATCH (n {age: 20}) RETURN n
            """
        Then the result should be:
            |              n                |
            |     (:Person {age: 20})       |
            |  (:Person :Student {age: 20}) |

    Scenario: Test match with order by
        Given an empty graph
        And having executed:
            """
            CREATE({a: 1}), ({a: 2}), ({a: 3}), ({a: 4}), ({a: 5})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a
            """
        Then the result should be, in order:
            | n.a |
            | 1   |
            | 2   |
            | 3   |
            | 4   |
            | 5   |

    Scenario: Test match with order by and skip
        Given an empty graph
        And having executed:
            """
            CREATE({a: 1}), ({a: 2}), ({a: 3}), ({a: 4}), ({a: 5})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a SKIP 3
            """
        Then the result should be, in order:
            | n.a |
            | 4   |
            | 5   |

    Scenario: Test match with order by and limit
        Given an empty graph
        And having executed:
            """
            CREATE({a: 1}), ({a: 2}), ({a: 3}), ({a: 4}), ({a: 5})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a LIMIT 2
            """
        Then the result should be, in order:
            | n.a |
            | 1   |
            | 2   |

    Scenario: Test match with order by, skip and limit
        Given an empty graph
        And having executed:
            """
            CREATE({a: 1}), ({a: 2}), ({a: 3}), ({a: 4}), ({a: 5})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a SKIP 2 LIMIT 2
            """
        Then the result should be, in order:
            | n.a |
            | 3   |
            | 4   |

    Scenario: Test match with order by and skip
        Given an empty graph
        And having executed:
            """
            CREATE({a: 1}), ({a: 2}), ({a: 3}), ({a: 4}), ({a: 5})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a SKIP 6
            """
        Then the result should be empty

    Scenario: Test match with order by and limit
        Given an empty graph
        And having executed:
            """
            CREATE({a: 1}), ({a: 2}), ({a: 3}), ({a: 4}), ({a: 5})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a LIMIT 0
            """
        Then the result should be empty

    Scenario: Test distinct
        Given an empty graph
        And having executed:
            """
            CREATE({a: 1}), ({a: 4}), ({a: 3}), ({a: 1}), ({a: 4})
            """
        When executing query:
            """
            MATCH (n) RETURN DISTINCT n.a
            """
        Then the result should be:
            | n.a |
            | 1   |
            | 3   |
            | 4   |

    Scenario: Test match unbounded variable path
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1}) -[:r]-> ({a:2}) -[:r]-> ({a:3})
            """
        When executing query:
            """
            MATCH (n) -[r*]-> (m) RETURN n.a, m.a
            """
        Then the result should be:
            | n.a | m.a |
            | 1   | 2   |
            | 1   | 3   |
            | 2   | 3   |

    Scenario: Test match 0 length variable path
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1}) -[:r]-> ({a:2}) -[:r]-> ({a:3})
            """
        When executing query:
            """
            MATCH (n) -[r*0]-> (m) RETURN n.a, m.a
            """
        Then the result should be:
            | n.a | m.a |
            | 1   | 1   |
            | 2   | 2   |
            | 3   | 3   |

    Scenario: Test match bounded variable path
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1}) -[:r]-> ({a:2}) -[:r]-> ({a:3})
            """
        When executing query:
            """
            MATCH (n) -[r*0..1]-> (m) RETURN n.a, m.a
            """
        Then the result should be:
            | n.a | m.a |
            | 1   | 1   |
            | 1   | 2   |
            | 2   | 2   |
            | 2   | 3   |
            | 3   | 3   |

    Scenario: Test match filtered edge type variable path
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1}) -[:r1]-> ({a:2}) -[:r2]-> ({a:3})
            """
        When executing query:
            """
            MATCH (n) -[:r1*]-> (m) RETURN n.a, m.a
            """
        Then the result should be:
            | n.a | m.a |
            | 1   | 2   |

    Scenario: Test match filtered properties variable path
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1}) -[:r {p1: 1, p2: 2}]-> ({a:2}) -[:r {p1: 1, p2: 3}]-> ({a:3})
            """
        When executing query:
            """
            MATCH (n) -[*{p1: 1, p2:2}]-> (m) RETURN n.a, m.a
            """
        Then the result should be:
            | n.a | m.a |
            | 1   | 2   |

    Scenario: Named path with length function.
        Given an empty graph
        And having executed:
            """
            CREATE (:start)-[:type]->()
            """
        When executing query:
            """
            MATCH path = (:start) -[*0..1]-> () RETURN size(path)
            """
        Then the result should be:
            | size(path) |
            | 0            |
            | 1            |
