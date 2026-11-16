Feature: MATCH after OPTIONAL MATCH

    Scenario: The reported shape crosses every optional row with every MATCH row
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (p:MaoP)-[:ACTED_IN]->(movies:MaoM)
            MATCH (m)<-[:ACTED_IN]-(person)
            RETURN p.name AS p, movies.title AS mv, person.name AS person, m.title AS m
            """
        Then the result should be:
            | p        | mv       | person   | m        |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'JW'     |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'JW'     |
            | 'Keanu'  | 'JW'     | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'JW'     | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'JW'     | 'Keanu'  | 'JW'     |

    Scenario: The reported shape crosses every optional row with every MATCH row (indexes :MaoP)
        Given an empty graph
        And with new index :MaoP
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (p:MaoP)-[:ACTED_IN]->(movies:MaoM)
            MATCH (m)<-[:ACTED_IN]-(person)
            RETURN p.name AS p, movies.title AS mv, person.name AS person, m.title AS m
            """
        Then the result should be:
            | p        | mv       | person   | m        |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'JW'     |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'JW'     |
            | 'Keanu'  | 'JW'     | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'JW'     | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'JW'     | 'Keanu'  | 'JW'     |

    Scenario: The reported shape crosses every optional row with every MATCH row (indexes :MaoP(name), :MaoM(title))
        Given an empty graph
        And with new index :MaoP(name)
        And with new index :MaoM(title)
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (p:MaoP)-[:ACTED_IN]->(movies:MaoM)
            MATCH (m)<-[:ACTED_IN]-(person)
            RETURN p.name AS p, movies.title AS mv, person.name AS person, m.title AS m
            """
        Then the result should be:
            | p        | mv       | person   | m        |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'JW'     |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'JW'     |
            | 'Keanu'  | 'JW'     | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'JW'     | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'JW'     | 'Keanu'  | 'JW'     |

    Scenario: The reported shape crosses every optional row with every MATCH row (indexes EDGE :ACTED_IN)
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :ACTED_IN
            """
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (p:MaoP)-[:ACTED_IN]->(movies:MaoM)
            MATCH (m)<-[:ACTED_IN]-(person)
            RETURN p.name AS p, movies.title AS mv, person.name AS person, m.title AS m
            """
        Then the result should be:
            | p        | mv       | person   | m        |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'JW'     |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'JW'     |
            | 'Keanu'  | 'JW'     | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'JW'     | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'JW'     | 'Keanu'  | 'JW'     |

    Scenario: An optional that matches nothing keeps one null row before the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone)
            MATCH (p:MaoP)
            RETURN x IS NULL AS xnull, p.name AS p
            """
        Then the result should be:
            | xnull | p        |
            | true  | 'Keanu'  |
            | true  | 'Carrie' |
            | true  | 'Hugo'   |

    Scenario: A MATCH expanding from a null optional node drops the row
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m)-[:SEQUEL]->(s)
            RETURN p.name AS p, m.title AS m, s.title AS s
            """
        Then the result should be:
            | p        | m        | s    |
            | 'Keanu'  | 'Matrix' | 'JW' |
            | 'Carrie' | 'Matrix' | 'JW' |

    Scenario: A MATCH expanding from a null optional node drops the row (indexes :MaoP)
        Given an empty graph
        And with new index :MaoP
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m)-[:SEQUEL]->(s)
            RETURN p.name AS p, m.title AS m, s.title AS s
            """
        Then the result should be:
            | p        | m        | s    |
            | 'Keanu'  | 'Matrix' | 'JW' |
            | 'Carrie' | 'Matrix' | 'JW' |

    Scenario: A MATCH expanding from a null optional node drops the row (indexes EDGE :SEQUEL)
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :SEQUEL
            """
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m)-[:SEQUEL]->(s)
            RETURN p.name AS p, m.title AS m, s.title AS s
            """
        Then the result should be:
            | p        | m        | s    |
            | 'Keanu'  | 'Matrix' | 'JW' |
            | 'Carrie' | 'Matrix' | 'JW' |

    Scenario: A MATCH expanding from a null optional node drops the row (indexes EDGE :ACTED_IN)
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :ACTED_IN
            """
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m)-[:SEQUEL]->(s)
            RETURN p.name AS p, m.title AS m, s.title AS s
            """
        Then the result should be:
            | p        | m        | s    |
            | 'Keanu'  | 'Matrix' | 'JW' |
            | 'Carrie' | 'Matrix' | 'JW' |

    Scenario: A variable-length MATCH from a null optional node drops the row
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m)-[*1..2]->(s)
            RETURN p.name AS p, m.title AS m, s.title AS s
            """
        Then the result should be:
            | p        | m        | s    |
            | 'Keanu'  | 'Matrix' | 'JW' |
            | 'Carrie' | 'Matrix' | 'JW' |

    Scenario: A lone node MATCH on a null optional node drops the row
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m)
            RETURN p.name AS p, m.title AS m
            """
        Then the result should be:
            | p        | m        |
            | 'Keanu'  | 'JW'     |
            | 'Keanu'  | 'Matrix' |
            | 'Carrie' | 'Matrix' |

    Scenario: The lone node null case through WITH, today's workaround spelling
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            WITH *
            MATCH (m)
            RETURN p.name AS p, m.title AS m
            """
        Then the result should be:
            | p        | m        |
            | 'Keanu'  | 'JW'     |
            | 'Keanu'  | 'Matrix' |
            | 'Carrie' | 'Matrix' |

    Scenario: A labelled MATCH on a null optional node drops the row
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m:MaoM)
            RETURN p.name AS p, m.title AS m
            """
        Then the result should be:
            | p        | m        |
            | 'Keanu'  | 'Matrix' |
            | 'Keanu'  | 'JW'     |
            | 'Carrie' | 'Matrix' |

    Scenario: A WHERE in the MATCH reads an optional variable
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        And parameters are:
            | lim | 3 |
        When executing query:
            """
            OPTIONAL MATCH (a:MaoM {title: 'JW'})
            MATCH (b:MaoM) WHERE b.v >= a.v AND b.v <= $lim
            RETURN a.title AS a, b.title AS b
            """
        Then the result should be:
            | a    | b      |
            | 'JW' | 'JW'   |
            | 'JW' | 'Lone' |

    Scenario: A WHERE on a null optional variable drops every row
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        And parameters are:
            | lim | 2 |
        When executing query:
            """
            OPTIONAL MATCH (a:MaoNone)
            MATCH (b:MaoM) WHERE b.v >= a.v
            RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: The later MATCH is not hoisted above the optional
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP {name: 'Hugo'})
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m2:MaoM {title: 'Matrix'})
            RETURN p.name AS p, m.title AS m, m2.title AS m2
            """
        Then the result should be:
            | p      | m    | m2       |
            | 'Hugo' | null | 'Matrix' |

    Scenario: The later MATCH is not hoisted above the optional (indexes :MaoM(title))
        Given an empty graph
        And with new index :MaoM(title)
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP {name: 'Hugo'})
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (m2:MaoM {title: 'Matrix'})
            RETURN p.name AS p, m.title AS m, m2.title AS m2
            """
        Then the result should be:
            | p      | m    | m2       |
            | 'Hugo' | null | 'Matrix' |

    Scenario: A relationship may repeat across the two MATCH clauses
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH ()-[r:SEQUEL]->()
            OPTIONAL MATCH (x:MaoNone)
            MATCH ()-[s:SEQUEL]->()
            RETURN count(*) AS c, r = s AS same
            """
        Then the result should be:
            | c | same |
            | 1 | true |

    Scenario: A relationship may repeat across two plain MATCH clauses
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH ()-[r:SEQUEL]->()
            MATCH ()-[s:SEQUEL]->()
            RETURN count(*) AS c, r = s AS same
            """
        Then the result should be:
            | c | same |
            | 1 | true |

    Scenario: Optional and MATCH alternate twice
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        And parameters are:
            | two | 2 |
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH (q:MaoP) WHERE q.name = p.name
            OPTIONAL MATCH (m)-[:SEQUEL]->(s)
            MATCH (any:MaoM) WHERE any.v >= $two
            RETURN p.name AS p, m.title AS m, s.title AS s, any.title AS l
            """
        Then the result should be:
            | p        | m        | s    | l      |
            | 'Keanu'  | 'JW'     | null | 'JW'   |
            | 'Keanu'  | 'JW'     | null | 'Lone' |
            | 'Keanu'  | 'Matrix' | 'JW' | 'JW'   |
            | 'Keanu'  | 'Matrix' | 'JW' | 'Lone' |
            | 'Carrie' | 'Matrix' | 'JW' | 'JW'   |
            | 'Carrie' | 'Matrix' | 'JW' | 'Lone' |
            | 'Hugo'   | null     | null | 'JW'   |
            | 'Hugo'   | null     | null | 'Lone' |

    Scenario: Several input rows reach the optional and the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        And parameters are:
            | names | ['Keanu', 'Hugo', 'Nobody'] |
        When executing query:
            """
            UNWIND $names AS n
            OPTIONAL MATCH (p:MaoP {name: n})
            MATCH (p)-[:ACTED_IN]->(m)
            RETURN n, count(m) AS c
            """
        Then the result should be:
            | n       | c |
            | 'Keanu' | 2 |

    Scenario: ORDER BY and LIMIT after the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        And parameters are:
            | k | 2 |
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone)
            MATCH (m:MaoM)
            RETURN m.title AS t ORDER BY t DESC LIMIT $k
            """
        Then the result should be, in order:
            | t        |
            | 'Matrix' |
            | 'Lone'   |

    Scenario: Inside a CALL subquery body
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            CALL (p) {
              OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
              MATCH (m)-[:SEQUEL]->(s)
              RETURN s.title AS s
            }
            RETURN p.name AS p, s
            """
        Then the result should be:
            | p        | s    |
            | 'Keanu'  | 'JW' |
            | 'Carrie' | 'JW' |

    Scenario: Inside a CALL subquery body (indexes EDGE :ACTED_IN)
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :ACTED_IN
            """
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            CALL (p) {
              OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
              MATCH (m)-[:SEQUEL]->(s)
              RETURN s.title AS s
            }
            RETURN p.name AS p, s
            """
        Then the result should be:
            | p        | s    |
            | 'Keanu'  | 'JW' |
            | 'Carrie' | 'JW' |

    Scenario: Inside a CALL subquery body (indexes EDGE :SEQUEL)
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :SEQUEL
            """
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            CALL (p) {
              OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
              MATCH (m)-[:SEQUEL]->(s)
              RETURN s.title AS s
            }
            RETURN p.name AS p, s
            """
        Then the result should be:
            | p        | s    |
            | 'Keanu'  | 'JW' |
            | 'Carrie' | 'JW' |

    Scenario: Inside an EXISTS body
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            RETURN p.name AS p, EXISTS {
              OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
              MATCH (m)-[:SEQUEL]->()
            } AS e
            """
        Then the result should be:
            | p        | e     |
            | 'Keanu'  | true  |
            | 'Carrie' | true  |
            | 'Hugo'   | false |

    Scenario: Inside a COUNT body
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            RETURN p.name AS p, COUNT {
              OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
              MATCH (n:MaoM)
            } AS c
            """
        Then the result should be:
            | p        | c |
            | 'Keanu'  | 6 |
            | 'Carrie' | 3 |
            | 'Hugo'   | 3 |

    Scenario: After a CALL subquery between the optional and the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone)
            CALL () { RETURN 1 AS one }
            MATCH (m:MaoM {title: 'Lone'})
            RETURN x IS NULL AS xnull, one, m.title AS m
            """
        Then the result should be:
            | xnull | one | m      |
            | true  | 1   | 'Lone' |

    Scenario: A SET after the MATCH writes once per surviving row
        Given an empty graph
        And having executed:
            """
            CREATE (:MaoW {id: 1, x: 0}), (:MaoW {id: 2, x: 0}), (:MaoV {id: 1})
            """
        When executing query:
            """
            MATCH (a:MaoW)
            OPTIONAL MATCH (v:MaoV {id: a.id})
            MATCH (b:MaoW {id: v.id})
            SET b.x = b.x + 1
            RETURN a.id AS a, b.x AS bx
            """
        Then the result should be:
            | a | bx |
            | 1 | 1  |

    Scenario: A CREATE after the MATCH is not read back by the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (:MaoC {id: 1}), (:MaoC {id: 2})
            """
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone)
            MATCH (c:MaoC)
            CREATE (:MaoC {id: c.id + 10})
            WITH count(*) AS created
            MATCH (t:MaoC)
            RETURN created, count(t) AS total
            """
        Then the result should be:
            | created | total |
            | 2       | 4     |

    Scenario: A MATCH after an update is still refused
        # Divergence: memgraph refuses MATCH after an update without WITH; Cypher allows it. Out of scope.
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone)
            CREATE (:MaoTmp)
            MATCH (m:MaoM)
            RETURN count(*) AS c
            """
        Then an error should be raised

    Scenario: The MATCH reuses a relationship variable the optional bound
        # Divergence: pre-existing "Redeclaring variable: r." for any reuse of a bound relationship variable in a later MATCH, WITH * too. Cypher returns a='Matrix', b='JW'.
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (a:MaoM)-[r:SEQUEL]->()
            MATCH ()-[r]->(b)
            RETURN a.title AS a, b.title AS b
            """
        Then an error should be raised

    Scenario: A named path in the MATCH starts at an optional node
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            MATCH q = (m)-[:SEQUEL]->()
            RETURN p.name AS p, length(q) AS len
            """
        Then the result should be:
            | p        | len |
            | 'Keanu'  | 1   |
            | 'Carrie' | 1   |

    Scenario: Each UNION leg may hold the pair
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone) MATCH (m:MaoM {title: 'Lone'}) RETURN m.title AS t
            UNION ALL
            OPTIONAL MATCH (p:MaoP {name: 'Hugo'}) MATCH (p)-[:ACTED_IN]->(m) RETURN m.title AS t
            """
        Then the result should be:
            | t      |
            | 'Lone' |

    Scenario: The MATCH reads what a later SET writes on another row
        Given an empty graph
        And having executed:
            """
            CREATE (:MaoW {id: 1, x: 0}), (:MaoW {id: 2, x: 0}), (:MaoV {id: 1}), (:MaoV {id: 2})
            """
        When executing query:
            """
            MATCH (a:MaoW)
            OPTIONAL MATCH (v:MaoV {id: a.id})
            MATCH (b:MaoW) WHERE b.x = 0
            SET b.x = b.x + 1
            RETURN count(*) AS c, sum(b.x) AS s
            """
        Then the result should be:
            | c | s |
            | 4 | 8 |

    Scenario: A writing CALL before the pair is seen by the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (:MaoW {id: 1})
            """
        When executing query:
            """
            CALL () { CREATE (:MaoT {id: 9}) }
            OPTIONAL MATCH (x:MaoNone)
            MATCH (t:MaoT)
            RETURN t.id AS t
            """
        Then the result should be:
            | t |
            | 9 |

    Scenario: An UNWIND between the optional and the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            MATCH (p:MaoP {name: 'Hugo'})
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
            UNWIND $xs AS x
            MATCH (m)-[:SEQUEL]->(s)
            RETURN x, s.title AS s
            """
        Then the result should be empty

    Scenario: An UNWIND of a non-node between the pair fails the node MATCH on it
        # Divergence: Cypher rejects this at compile time (n is an integer); memgraph raises at run time "Expected a node for 'n', but got int.", as with WITH between them.
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone)
            UNWIND [1] AS n
            MATCH (n)
            RETURN n
            """
        Then an error should be raised

    Scenario: A read procedure call between the optional and the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone)
            CALL mg.procedures() YIELD name
            WITH x, count(name) > 0 AS any
            MATCH (m:MaoM {title: 'Lone'})
            RETURN x IS NULL AS xnull, any, m.title AS m
            """
        Then the result should be:
            | xnull | any  | m      |
            | true  | true | 'Lone' |

    Scenario: A read procedure call directly between the optional and the MATCH
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            OPTIONAL MATCH (x:MaoNone)
            CALL mg.procedures() YIELD name
            MATCH (m:MaoM {title: 'Lone'})
            RETURN x IS NULL AS xnull, count(name) > 0 AS any, m.title AS m
            """
        Then the result should be:
            | xnull | any  | m      |
            | true  | true | 'Lone' |

    Scenario: Inside a COLLECT body
        Given an empty graph
        And having executed:
            """
            CREATE (k:MaoP {name: 'Keanu'}), (c:MaoP {name: 'Carrie'}), (h:MaoP {name: 'Hugo'}),
                   (mx:MaoM {title: 'Matrix', v: 1}), (jw:MaoM {title: 'JW', v: 2}), (lone:MaoM {title: 'Lone', v: 3}),
                   (k)-[:ACTED_IN]->(mx), (c)-[:ACTED_IN]->(mx), (k)-[:ACTED_IN]->(jw),
                   (mx)-[:SEQUEL]->(jw)
            """
        When executing query:
            """
            MATCH (p:MaoP)
            RETURN p.name AS p, size(COLLECT {
              OPTIONAL MATCH (p)-[:ACTED_IN]->(m:MaoM)
              MATCH (m)-[:SEQUEL]->(s)
              RETURN s.title
            }) AS c
            """
        Then the result should be:
            | p        | c |
            | 'Keanu'  | 1 |
            | 'Carrie' | 1 |
            | 'Hugo'   | 0 |
