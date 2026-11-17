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
            MATCH q = (m)-[:SEQUEL]->(s)
            RETURN p.name AS p, m.title AS m, s.title AS s, length(q) AS len
            """
        Then the result should be:
            | p        | m        | s    | len |
            | 'Keanu'  | 'Matrix' | 'JW' | 1   |
            | 'Carrie' | 'Matrix' | 'JW' | 1   |

    Scenario: A MATCH expanding from a null optional node drops the row (indexes EDGE :SEQUEL)
        Given an empty graph
        And with new edge type index :SEQUEL
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
            MATCH q = (m)-[:SEQUEL]->(s)
            RETURN p.name AS p, m.title AS m, s.title AS s, length(q) AS len
            """
        Then the result should be:
            | p        | m        | s    | len |
            | 'Keanu'  | 'Matrix' | 'JW' | 1   |
            | 'Carrie' | 'Matrix' | 'JW' | 1   |

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
            | lim | 2 |
        When executing query:
            """
            OPTIONAL MATCH (a:MaoM {title: 'JW'})
            MATCH (b:MaoM) WHERE b.v >= a.v AND b.v <= $lim
            RETURN a.title AS a, b.title AS b
            """
        Then the result should be:
            | a    | b    |
            | 'JW' | 'JW' |

    Scenario: An independent MATCH after a null optional keeps the null
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

    Scenario: Optional and MATCH alternate twice
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
            MATCH (m)<-[:ACTED_IN]-(q:MaoP)
            OPTIONAL MATCH (m)-[:SEQUEL]->(s)
            MATCH (s)<-[:SEQUEL]-(back)
            RETURN p.name AS p, m.title AS m, q.name AS q, back.title AS b
            """
        Then the result should be:
            | p        | m        | q        | b        |
            | 'Keanu'  | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Keanu'  | 'Matrix' | 'Keanu'  | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Carrie' | 'Matrix' |
            | 'Carrie' | 'Matrix' | 'Keanu'  | 'Matrix' |

    Scenario: Inside a CALL subquery body (indexes EDGE :SEQUEL)
        Given an empty graph
        And with new edge type index :SEQUEL
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

    Scenario: Inside a CALL subquery body that imports nothing
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
            CALL {
              OPTIONAL MATCH (q:MaoP {name: 'Hugo'})-[:ACTED_IN]->(m:MaoM)
              MATCH (m)-[:SEQUEL]->(s)
              RETURN count(*) AS c
            }
            RETURN p.name AS p, c
            """
        Then the result should be:
            | p        | c |
            | 'Keanu'  | 0 |
            | 'Carrie' | 0 |
            | 'Hugo'   | 0 |

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

    Scenario: The MATCH does not see what the SET wrote on an earlier row
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
