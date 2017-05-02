Feature: With

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
        When executing query:
            """
            CREATE(a:A), (b:B), (c:C), (a)-[:T]->(b) WITH a DETACH DELETE a WITH a MATCH()-[r:T]->() RETURN r
            """
        Then an error should be raised
