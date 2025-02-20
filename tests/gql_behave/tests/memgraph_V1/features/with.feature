Feature: With

    Scenario: With test 01:
        Given an empty graph
        And having executed:
            """
            CREATE (a:A), (b:B), (c:C), (d:D), (e:E), (a)-[:R]->(b), (b)-[:R]->(c), (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
            """
        When executing query:
            """
            MATCH (:A)--(a)-->() WITH a, COUNT(*) AS n WHERE n > 1 RETURN a
            """
        Then the result should be:
            | a    |
            | (:B) |

    Scenario: With test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d), (d)-[:R]->(a)
            """
        When executing query:
            """
            MATCH (a)--(b)
            WITH a, MAX(b.x) AS s
            RETURN a, s
            """
        Then the result should be:
            | a          |  s  |
            | (:A{x: 1}) |  4  |
            | (:B{x: 2}) |  3  |
            | (:C{x: 3}) |  4  |
            | (:D{x: 4}) |  3  |

    Scenario: With test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a:A{x: 1}), (b:B{x: 2}), (a)-[:R]->(b), (a)-[:R]->(b), (b)-[:R]->(a), (b)-[:R]->(a)
            """
        When executing query:
            """
            MATCH (b)--(a)--(c)
            WITH a, (SUM(b.x)+SUM(c.x)) AS s
            RETURN a, s
            """
        Then the result should be:
            | a          | s  |
            | (:A{x: 1}) | 48 |
            | (:B{x: 2}) | 24 |

    Scenario: With test 04:
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c), (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
            """
        When executing query:
            """
            MATCH (c)--(a:B)--(b)--(d)
            WITH a, b, SUM(c.x)+SUM(d.x) AS n RETURN a, b, n
            """
        Then the result should be:
            | a          | b          | n   |
            | (:B{x: 2}) | (:A{x: 1}) | 13  |
            | (:B{x: 2}) | (:C{x: 3}) | 22  |
            | (:B{x: 2}) | (:D{x: 4}) | 14  |

    Scenario: With test 05:
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c), (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
            """
        When executing query:
            """
            MATCH (c)--(a:B)--(b)--(d)
            WITH a, b, AVG(c.x + d.x) AS n RETURN a, b, n
            """
        Then the result should be:
            | a          | b          | n   |
            | (:B{x: 2}) | (:A{x: 1}) | 6.5 |
            | (:B{x: 2}) | (:C{x: 3}) | 5.5 |
            | (:B{x: 2}) | (:D{x: 4}) | 7.0 |

    Scenario: With test 06:
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c), (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
            """
        When executing query:
            """
            MATCH (c)--(a:B)--(b)--(d)
            WITH a, b, AVG(c.x + d.x) AS n RETURN MAX(n) AS n
            """
        Then the result should be:
            | n   |
            | 7.0 |

    Scenario: With test 07:
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c), (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
            """
        When executing query:
            """
            MATCH (c)--(a:B)--(b)--(d)
            WITH a, b, AVG(c.x + d.x) AS n
            WITH a, MAX(n) AS n RETURN a, n
            """
        Then the result should be:
            | a          | n   |
            | (:B{x: 2}) | 7.0 |

    Scenario: With test 08:
        Given an empty graph
        When executing query:
            """
            CREATE (a), (b) WITH a, b CREATE (a)-[r:R]->(b) RETURN r
            """
        Then the result should be:
            | r    |
            | [:R] |

    Scenario: With test 09:
        Given an empty graph
        When executing query:
            """
            CREATE (a), (b) WITH a, b SET a:X SET b:Y WITH a, b MATCH(x:X) RETURN x
            """
        Then the result should be:
            | x    |
            | (:X) |

    Scenario: With test 10:
        Given an empty graph
        When executing query:
            """
            CREATE (a), (b), (a)-[:R]->(b) WITH a, b SET a:X SET b:Y
            WITH a MATCH(x:X)--(b) RETURN x, x AS y
            """
        Then the result should be:
            | x    | y    |
            | (:X) | (:X) |

    Scenario: With test 10:
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c), (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
            """
        When executing query:
            """
            MATCH (c)--(a:B)--(b)--(d) WITH a, b, AVG(c.x + d.x) AS av WITH AVG(av) AS avg
            MATCH (c)--(a:B)--(b)--(d) WITH a, b, avg, AVG(c.x + d.x) AS av WHERE av>avg RETURN av
            """
        Then the result should be:
            | av  |
            | 6.5 |
            | 7.0 |

    Scenario: With test 11:
        Given an empty graph
        And having executed:
            """
            CREATE(:A{a: 1}), (:B{a: 1}), (:C{a: 1}), (:D{a: 4}), (:E{a: 5})
            """
        When executing query:
            """
            MATCH(n) WITH n.a AS a
            ORDER BY a LIMIT 4
            RETURN a
            """
        Then the result should be, in order:
            | a |
            | 1 |
            | 1 |
            | 1 |
            | 4 |

    Scenario: With test 12:
        Given an empty graph
        And having executed:
            """
            CREATE(:A{a: 1}), (:B{a: 5}), (:C{a: 2}), (:D{a: 3}), (:E{a: 5})
            """
        When executing query:
            """
            MATCH(n) WITH n.a AS a
            ORDER BY a SKIP 2
            RETURN a
            """
        Then the result should be, in order:
            | a |
            | 3 |
            | 5 |
            | 5 |

    Scenario: With test 13:
        Given an empty graph
        And having executed:
            """
            CREATE(:A{a: 1}), (:B{a: 5}), (:C{a: 2}), (:D{a: 3}), (:E{a: 5})
            """
        When executing query:
            """
            MATCH(n) WITH n.a AS a
            ORDER BY a
            RETURN a
            """
        Then the result should be, in order:
            | a |
            | 1 |
            | 2 |
            | 3 |
            | 5 |
            | 5 |

    Scenario: With test 14:
        Given an empty graph
        And having executed:
            """
            CREATE(:A{a: 1}), (:B{a: 5}), (:C{a: 1}), (:D{a: 3}), (:E{a: 5})
            """
        When executing query:
            """
            MATCH(n) WITH DISTINCT n.a AS a
            RETURN a
            """
        Then the result should be:
            | a |
            | 1 |
            | 3 |
            | 5 |

    Scenario: With test 15:
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 0})
            """
        When executing query:
            """
            MATCH (n) WITH n RETURN *
            """
        Then the result should be:
            | n         |
            | ({id: 0}) |

    Scenario: With test 16:
        Given an empty graph
        And having executed:
            """
            CREATE ({id: 0}) CREATE ({id:1});
            """
        When executing query:
            """
            MATCH (n) RETURN n.id AS id;
            """
        Then the result should be:
            | id |
            | 0  |
            | 1  |

    Scenario: With test 17:
        Given an empty graph
        And having executed:
            """
            CREATE ({name: "node1"})
            """
        When executing query:
            """
            MATCH (n) WITH n AS node
            CREATE (m:node.name)
            """
        When executing query:
            """
            MATCH (n:node1) RETURN n;
            """
        Then the result should be:
            | n        |
            | (:node1) |

    Scenario: With test 18:
        Given an empty graph
        And having executed:
            """
            CREATE ({name: "LabelToAdd"})
            """
        When executing query:
            """
            MATCH (n) WITH n AS node
            SET node:node.name
            """
        When executing query:
            """
            MATCH (n) RETURN n;
            """
        Then the result should be:
            | n                                  |
            | (:LabelToAdd {name: 'LabelToAdd'}) |

    Scenario: With test 19:
        Given an empty graph
        And having executed:
            """
            CREATE (:labelToRemove {name: 'labelToRemove'})
            """
        When executing query:
            """
            MATCH (n) WITH n AS node
            REMOVE node:node.name
            """
        When executing query:
            """
            MATCH (n) RETURN n;
            """
        Then the result should be:
            | n                         |
            | ({name: 'labelToRemove'}) |

    Scenario: With test 20:
        Given an empty graph
        And having executed:
            """
            CREATE ({name: 'label1'})
            """
        When executing query:
            """
            MATCH (n) WITH n AS node
            SET node:node.name:label2
            """
        When executing query:
            """
            MATCH (n) RETURN n;
            """
        Then the result should be:
            | n                                 |
            | (:label1:label2 {name: 'label1'}) |

    Scenario: With test 21:
        Given an empty graph
        And having executed:
            """
            CREATE ({name: 'label1'})
            """
        When executing query:
            """
            MATCH (n) WITH n AS node
            SET node:label2:node.name
            """
        When executing query:
            """
            MATCH (n) RETURN n;
            """
        Then the result should be:
            | n                                 |
            | (:label2:label1 {name: 'label1'}) |

    Scenario: With test 22:
        Given an empty graph
        And having executed:
            """
            WITH {value: {label: "labelvalue"}} as label
            CREATE (n:label.value.label);
            """
        When executing query:
            """
            MATCH (n) RETURN n;
            """
        Then the result should be:
            | n             |
            | (:labelvalue) |

    Scenario: With test 23:
        Given an empty graph
        And having executed:
            """
            CREATE ({name: "node1"})
            """
        When executing query:
            """
            MATCH (n) WITH n AS node
            CREATE ()-[:node.name]->()
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN r;
            """
        Then the result should be:
            | r        |
            | [:node1] |

    Scenario: With test 24 (dynamic not ):
        Given an empty graph
        And having executed:
            """
            CREATE ({name: "node1"})
            """
        When executing query:
            """
            MATCH (n) WITH n AS node
            CREATE ()-[:node.name]->()
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN r;
            """
        Then the result should be:
            | r        |
            | [:node1] |

    Scenario: With test 25 (dynamic node labels not being able to pass match):
        Given an empty graph
        When executing query:
            """
            WITH {value: {labelz: "labelz"}} as labelz
            MATCH (n:labelz.value.labelz) RETURN n;
            """
        Then an error should be raised

    Scenario: With test 26 (dynamic node labels not being able to pass merge):
        Given an empty graph
        When executing query:
            """
            WITH {value: {labelz: "labelz"}} as labelz
            MERGE (n:labelz.value.labelz);
            """
        Then an error should be raised

    Scenario: With test 27 (dynamic edge type not able to pass match):
        Given an empty graph
        When executing query:
            """
            WITH {value: {edge_type: "edgetypevalue"}} as edge_type
            MATCH ()-[r:edge_type.value.edge_type]->();
            """
        Then an error should be raised

    Scenario: With test 28 (dynamic edge type not able to pass merge):
        Given an empty graph
        When executing query:
            """
            WITH {value: {edge_type: "edgetypevalue"}} as edge_type
            MERGE ()-[r:edge_type.value.edge_type]->();
            """
        Then an error should be raised
