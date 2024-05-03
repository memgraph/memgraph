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

    Scenario: Create and match pattern with cross referencing variables in property maps
        Given an empty graph
        And having executed:
            """
            CREATE ({x: 1, y: 5, z: 3})-[:E]->({x: 10, y: 1, z: 3})
            """
        When executing query:
            """
            MATCH (n {x: m.y, z: m.z})-[]-(m {y: n.x, z: n.z}) RETURN n, m
            """
        Then the result should be:
            |            n         |           m           |
            | ({x: 1, y: 5, z: 3}) | ({x: 10, y: 1, z: 3}) |

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

    Scenario: Test match with order by and date
        Given an empty graph
        And having executed:
            """
            CREATE({a: DATE('2021-12-31')}), ({a: DATE('2021-11-11')}), ({a: DATE('2021-12-28')})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a
            """
        Then the result should be, in order:
            | n.a        |
            | 2021-11-11 |
            | 2021-12-28 |
            | 2021-12-31 |

    Scenario: Test match with order by and localtime
        Given an empty graph
        And having executed:
            """
            CREATE({a: LOCALTIME('09:12:31')}), ({a: LOCALTIME('09:09:20')}), ({a: LOCALTIME('09:11:21')})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a
            """
        Then the result should be, in order:
            | n.a                |
            | 09:09:20.000000000 |
            | 09:11:21.000000000 |
            | 09:12:31.000000000 |

    Scenario: Test match with order by and localdatetime
        Given an empty graph
        And having executed:
            """
            CREATE({a: LOCALDATETIME('2021-11-22T09:12:31')}), ({a: LOCALDATETIME('2021-11-23T09:10:30')}), ({a: LOCALDATETIME('2021-11-10T09:14:21')})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a
            """
        Then the result should be, in order:
            | n.a                           |
            | 2021-11-10T09:14:21.000000000 |
            | 2021-11-22T09:12:31.000000000 |
            | 2021-11-23T09:10:30.000000000 |

    Scenario: Test match with order by and duration
        Given an empty graph
        And having executed:
            """
            CREATE({a: DURATION('P12DT3M')}), ({a: DURATION('P11DT8M')}), ({a: DURATION('P11DT60H')})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a
            """
        Then the result should be, in order:
            | n.a      |
            | P11DT8M  |
            | P12DT3M  |
            | P13DT12H |

    Scenario: Test match with order by and datetime
        Given an empty graph
        And having executed:
            """
            CREATE({a: DATETIME('2024-01-22T08:11:31[Etc/UTC]')}),
                  ({a: DATETIME('2024-01-22T08:12:31[Etc/UTC]')}),
                  ({a: DATETIME('2024-01-22T08:42:31+00:30')}),
                  ({a: DATETIME('2024-01-22T08:57:31+00:45')}),
                  ({a: DATETIME('2024-01-22T09:12:31[Europe/Zurich]')}),
                  ({a: DATETIME('2024-01-22T09:12:31[Europe/Warsaw]')})
            """
        When executing query:
            """
            MATCH (n) RETURN n.a ORDER BY n.a
            """
        Then the result should be, in order:
            | n.a                                 |
            | 2024-01-22T08:11:31.000000000+00:00 |
            | 2024-01-22T08:12:31.000000000+00:00 |
            | 2024-01-22T08:42:31.000000000+00:30 |
            | 2024-01-22T08:57:31.000000000+00:45 |
            | 2024-01-22T09:12:31.000000000+01:00 |
            | 2024-01-22T09:12:31.000000000+01:00 |

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
            CREATE (:starting)-[:type]->()
            """
        When executing query:
            """
            MATCH path = (:starting) -[*0..1]-> () RETURN size(path)
            """
        Then the result should be:
            | size(path) |
            | 0          |
            | 1          |

    Scenario: Variable expand to existing symbol 1
        Given an empty graph
        And having executed:
            """
            CREATE (p1:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4})-[:KNOWS]->(p1);
            """
        When executing query:
            """
            MATCH path = (pers:Person {id: 3})-[:KNOWS*2]->(pers) RETURN path;
            """
        Then the result should be empty

    Scenario: Variable expand to existing symbol 2
        Given an empty graph
        And having executed:
            """
            CREATE (p1:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4})-[:KNOWS]->(p1);
            """
        When executing query:
            """
            MATCH path = (pers:Person {id: 3})-[:KNOWS*]->(pers) RETURN path
            """
        Then the result should be:
            | path                                                                                                                      |
            | <(:Person{id:3})-[:KNOWS]->(:Person{id:4})-[:KNOWS]->(:Person{id:1})-[:KNOWS]->(:Person{id:2})-[:KNOWS]->(:Person{id:3})> |

    Scenario: Variable expand to existing symbol 3
        Given an empty graph
        And having executed:
            """
            CREATE (p1:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4})-[:KNOWS]->(p1);
            """
        When executing query:
            """
            MATCH path = (pers:Person {id: 3})-[:KNOWS*0..]->(pers) RETURN path
            """
        Then the result should be:
            | path                                                                                                                      |
            | <(:Person{id:3})>                                                                                                         |
            | <(:Person{id:3})-[:KNOWS]->(:Person{id:4})-[:KNOWS]->(:Person{id:1})-[:KNOWS]->(:Person{id:2})-[:KNOWS]->(:Person{id:3})> |

    Scenario: Variable expand to existing symbol 4
        Given an empty graph
        And having executed:
            """
            CREATE (p1:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4})-[:KNOWS]->(p1);
            """
        When executing query:
            """
            MATCH path = (pers:Person {id: 3})-[:KNOWS*2..6]->(pers) RETURN path
            """
        Then the result should be:
            | path                                                                                                                      |
            | <(:Person{id:3})-[:KNOWS]->(:Person{id:4})-[:KNOWS]->(:Person{id:1})-[:KNOWS]->(:Person{id:2})-[:KNOWS]->(:Person{id:3})> |

    Scenario: Variable expand to existing symbol 5
        Given an empty graph
        And having executed:
            """
            CREATE (p1:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4})-[:KNOWS]->(p1);
            """
        When executing query:
            """
            MATCH path = (pers:Person {id: 3})-[:KNOWS*5..]->(pers) RETURN path
            """
        Then the result should be empty

    Scenario: Variable expand to existing symbol 6
        Given an empty graph
        And having executed:
            """
            CREATE (p1:Person {id: 1})-[:KNOWS]->(p1);
            """
        When executing query:
            """
            MATCH path = (pers:Person {id: 1})-[:KNOWS*]->(pers) RETURN path
            """
        Then the result should be
            | path                                        |
            | <(:Person{id:1})-[:KNOWS]->(:Person{id:1})> |

    Scenario: Variable expand to existing symbol 7
        Given an empty graph
        And having executed:
            """
            CREATE (p1:Person {id: 1})-[:KNOWS]->(p1);
            """
        When executing query:
            """
            MATCH path = (pers:Person {id: 1})-[:KNOWS*0..]->(pers) RETURN path
            """
        Then the result should be
            | path                                        |
            | <(:Person{id:1})>                           |
            | <(:Person{id:1})-[:KNOWS]->(:Person{id:1})> |

    Scenario: Variable expand to existing symbol 8
        Given an empty graph
        And having executed:
            """
            CREATE (p1:Person {id: 1})-[:KNOWS]->(p1);
            """
        When executing query:
            """
            MATCH path = (pers:Person {id: 1})-[:KNOWS*2..]->(pers) RETURN path
            """
            Then the result should be empty

    Scenario: Match with temporal property
        Given an empty graph
        And having executed:
            """
            CREATE (n:User {time: localDateTime("2021-10-05T14:15:00")});
            """
        When executing query:
            """
            MATCH (n) RETURN date(n.time);
            """
        Then the result should be
            | date(n.time) |
            | 2021-10-05   |

    Scenario: Variable expand with filter by size of accumulated path
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4});
            """
        When executing query:
            """
            MATCH path = (:Person {id: 1})-[* (e, n, p | size(p) < 4)]->(:Person {id: 4}) RETURN path
            """
        Then the result should be
            | path                                        |
            | <(:Person{id:1})-[:KNOWS]->(:Person{id:2})-[:KNOWS]->(:Person{id:3})-[:KNOWS]->(:Person{id:4})> |

    Scenario: Variable expand with filter by last edge type of accumulated path
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4});
            """
        When executing query:
            """
            MATCH path = (:Person {id: 1})-[* (e, n, p | type(relationships(p)[-1]) = 'KNOWS')]->(:Person {id: 4}) RETURN path
            """
        Then the result should be
            | path                                        |
            | <(:Person{id:1})-[:KNOWS]->(:Person{id:2})-[:KNOWS]->(:Person{id:3})-[:KNOWS]->(:Person{id:4})> |

    Scenario: Variable expand with too restricted filter by size of accumulated path
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4});
            """
        When executing query:
            """
            MATCH path = (:Person {id: 1})-[* (e, n, p | size(p) < 3)]->(:Person {id: 4}) RETURN path
            """
        Then the result should be empty

    Scenario: Variable expand with too restricted filter by last edge type of accumulated path
        Given an empty graph
        And having executed:
            """
            CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})-[:KNOWS]->(:Person {id: 3})-[:KNOWS]->(:Person {id: 4});
            """
        When executing query:
            """
            MATCH path = (:Person {id: 1})-[* (e, n, p | type(relationships(p)[-1]) = 'Invalid')]->(:Person {id: 4}) RETURN path
            """
        Then the result should be empty

    Scenario: Test DFS variable expand with filter by edge type1
        Given graph "graph_edges"
        When executing query:
            """
            MATCH path=(:label1)-[* (e, n, p | NOT(type(e)='type1' AND type(last(relationships(p))) = 'type1'))]->(:label3) RETURN path;
            """
        Then the result should be:
            | path                                        |
            | <(:label1 {id: 1})-[:type2 {id: 10}]->(:label3 {id: 3})> |
            | <(:label1 {id: 1})-[:same {id: 30}]->(:label1 {id: 1})-[:type2 {id: 10}]->(:label3 {id: 3})> |

    Scenario: Test DFS variable expand using IN edges with filter by edge type1
        Given graph "graph_edges"
        When executing query:
            """
            MATCH path=(:label3)<-[* (e, n, p | NOT(type(e)='type1' AND type(last(relationships(p))) = 'type1'))]-(:label1) RETURN path;
            """
        Then the result should be:
            | path                                        |
            | <(:label3 {id: 3})<-[:type2 {id: 10}]-(:label1 {id: 1})> |
            | <(:label3 {id: 3})<-[:type2 {id: 10}]-(:label1 {id: 1})-[:same {id: 30}]->(:label1 {id: 1})> |

    Scenario: Test DFS variable expand with filter by edge type2
        Given graph "graph_edges"
        When executing query:
            """
            MATCH path=(:label1)-[* (e, n, p | NOT(type(e)='type2' AND type(last(relationships(p))) = 'type2'))]->(:label3) RETURN path;
            """
        Then the result should be:
            | path                                        |
            | <(:label1 {id: 1})-[:type1 {id: 1}]->(:label2 {id: 2})-[:type1 {id: 2}]->(:label3 {id: 3})> |
            | <(:label1 {id: 1})-[:same {id: 30}]->(:label1 {id: 1})-[:type1 {id: 1}]->(:label2 {id: 2})-[:type1 {id: 2}]->(:label3 {id: 3})> |

    Scenario: Test DFS variable expand using IN edges with filter by edge type2
        Given graph "graph_edges"
        When executing query:
            """
            MATCH path=(:label3)<-[* (e, n, p | NOT(type(e)='type2' AND type(last(relationships(p))) = 'type2'))]-(:label1) RETURN path;
            """
        Then the result should be:
            | path                                        |
            | <(:label3 {id: 3})<-[:type1 {id: 2}]-(:label2 {id: 2})<-[:type1 {id: 1}]-(:label1 {id: 1})> |
            | <(:label3 {id: 3})<-[:type1 {id: 2}]-(:label2 {id: 2})<-[:type1 {id: 1}]-(:label1 {id: 1})-[:same {id: 30}]->(:label1 {id: 1})> |

    Scenario: Using path indentifier from CREATE in MERGE
        Given an empty graph
        And having executed:
            """
            CREATE p0=()-[:T0]->() MERGE ({k:(size(p0))});
            """
        When executing query:
            """
            MATCH (n {k: 1}) RETURN n;
            """
        Then the result should be:
            | n        |
            | ({k: 1}) |
