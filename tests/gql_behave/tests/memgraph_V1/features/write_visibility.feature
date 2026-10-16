Feature: Write visibility inside one query

    Scenario: EXISTS after SET in the same part sees the new property
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k RETURN EXISTS { MATCH (b:WvP) WHERE b.flag = 1 } AS e
            """
        Then the result should be:
            | e    |
            | true |

    Scenario: EXISTS after SET in the same part sees the new property (indexes :WvP(flag))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvP(flag)
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k RETURN EXISTS { MATCH (b:WvP) WHERE b.flag = 1 } AS e
            """
        Then the result should be:
            | e    |
            | true |

    Scenario: COUNT after SET sees the writes of every row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (a:WvS {id: i}) SET a.x = a.k RETURN collect(COUNT { MATCH (b:WvS) WHERE b.x = 1 }) AS r
            """
        Then the result should be:
            | r      |
            | [2, 2] |

    Scenario: COUNT after SET sees the writes of every row (indexes :WvS(x))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvS(x)
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (a:WvS {id: i}) SET a.x = a.k RETURN collect(COUNT { MATCH (b:WvS) WHERE b.x = 1 }) AS r
            """
        Then the result should be:
            | r      |
            | [2, 2] |

    Scenario: COLLECT after SET sees the new property
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k RETURN COLLECT { MATCH (b:WvP) WHERE b.flag = 1 RETURN b.name } AS r
            """
        Then the result should be:
            | r     |
            | ['A'] |

    Scenario: Correlated EXISTS after SET label sees the new label
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a:WvT RETURN EXISTS { MATCH (a) WHERE a:WvT } AS r
            """
        Then the result should be:
            | r    |
            | true |

    Scenario: Pattern comprehension after SET sees the new property
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k RETURN size([(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name]) AS r
            """
        Then the result should be:
            | r |
            | 1 |

    Scenario: Pattern comprehension after SET sees the new property (indexes :WvP(flag))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvP(flag)
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k RETURN size([(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name]) AS r
            """
        Then the result should be:
            | r |
            | 1 |

    Scenario: List comprehension after SET sees the new property
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN [n IN [a] WHERE n.x = 1 | n.id] AS r
            """
        Then the result should be:
            | r   |
            | [1] |

    Scenario: List comprehension over collect after SET and WITH sees every row
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS) SET a.x = a.k WITH collect(a) AS ns RETURN size([n IN ns WHERE n.x = 1]) AS r
            """
        Then the result should be:
            | r |
            | 2 |

    Scenario: COUNT after SET and WITH sees every row
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS) SET a.x = a.k WITH count(*) AS n RETURN n, COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r
            """
        Then the result should be:
            | n | r |
            | 2 | 2 |

    Scenario: COUNT after SET and WITH sees every row (indexes :WvS(x))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvS(x)
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS) SET a.x = a.k WITH count(*) AS n RETURN n, COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r
            """
        Then the result should be:
            | n | r |
            | 2 | 2 |

    Scenario: Scoped CALL reads the CREATE before it
        # Pre-flight: new
        Given an empty graph
        When executing query:
            """
            CREATE (:WvX) CALL () { MATCH (x:WvX) RETURN count(x) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: Scoped CALL reads the CREATE before it (indexes :WvX)
        # Pre-flight: new
        Given an empty graph
        And with new index :WvX
        When executing query:
            """
            CREATE (:WvX) CALL () { MATCH (x:WvX) RETURN count(x) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: Unscoped CALL reads the CREATE before it
        # Pre-flight: new
        Given an empty graph
        When executing query:
            """
            CREATE (:WvX) CALL { MATCH (x:WvX) RETURN count(x) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: CALL after CREATE and WITH reads the CREATE
        # Pre-flight: existing
        Given an empty graph
        When executing query:
            """
            CREATE (:WvX) WITH * CALL () { MATCH (x:WvX) RETURN count(x) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: MATCH after a nested writing CALL inside a CALL body sees the CREATE
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvI {id: 0})
            """
        When executing query:
            """
            CALL () { CALL (*) { CREATE (:WvI {id: 1}) RETURN 1 AS x } MATCH (n:WvI) RETURN count(n) AS rows } RETURN rows
            """
        Then the result should be:
            | rows |
            | 2    |

    Scenario: MATCH after a nested writing CALL inside a CALL body sees the CREATE (indexes :WvI(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvI(id)
        And having executed:
            """
            CREATE (:WvI {id: 0})
            """
        When executing query:
            """
            CALL () { CALL (*) { CREATE (:WvI {id: 1}) RETURN 1 AS x } MATCH (n:WvI) RETURN count(n) AS rows } RETURN rows
            """
        Then the result should be:
            | rows |
            | 2    |

    Scenario: MATCH after a writing CALL at top level sees the CREATE
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvI {id: 0})
            """
        When executing query:
            """
            CALL (*) { CREATE (:WvI {id: 1}) RETURN 1 AS x } MATCH (n:WvI) RETURN count(n) AS c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: MATCH after a writing CALL at top level sees the CREATE (indexes :WvI(id))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvI(id)
        And having executed:
            """
            CREATE (:WvI {id: 0})
            """
        When executing query:
            """
            CALL (*) { CREATE (:WvI {id: 1}) RETURN 1 AS x } MATCH (n:WvI) RETURN count(n) AS c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: CALL body WITH WHERE sees its own write for an earlier row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvB {x: 0})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (b:WvB) CALL (b, i) { WITH b, i WHERE b.x = 0 SET b.x = i RETURN i AS r } RETURN collect(r) AS rs, collect(b.x) AS xs
            """
        Then the result should be:
            | rs  | xs  |
            | [1] | [1] |

    Scenario: CALL body MATCH WHERE sees its own write for an earlier row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvB {x: 0})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i CALL (i) { MATCH (b:WvB) WHERE b.x = 0 SET b.x = i RETURN i AS r } WITH collect(r) AS rs MATCH (b:WvB) RETURN rs, b.x AS x
            """
        Then the result should be:
            | rs  | x |
            | [1] | 1 |

    Scenario: CALL body MATCH WHERE sees its own write for an earlier row (indexes :WvB(x))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvB(x)
        And having executed:
            """
            CREATE (:WvB {x: 0})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i CALL (i) { MATCH (b:WvB) WHERE b.x = 0 SET b.x = i RETURN i AS r } WITH collect(r) AS rs MATCH (b:WvB) RETURN rs, b.x AS x
            """
        Then the result should be:
            | rs  | x |
            | [1] | 1 |

    Scenario: CALL body count sees the CREATEs of earlier rows
        # Pre-flight: new
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2, 3] AS i CALL (i) { MATCH (n:WvB) WITH count(n) AS c CREATE (:WvB) RETURN c } RETURN collect(c) AS cs
            """
        Then the result should be:
            | cs        |
            | [0, 1, 2] |

    Scenario: CALL body count sees the CREATEs of earlier rows (indexes :WvB)
        # Pre-flight: new
        Given an empty graph
        And with new index :WvB
        When executing query:
            """
            UNWIND [1, 2, 3] AS i CALL (i) { MATCH (n:WvB) WITH count(n) AS c CREATE (:WvB) RETURN c } RETURN collect(c) AS cs
            """
        Then the result should be:
            | cs        |
            | [0, 1, 2] |

    Scenario: CALL body with a disconnected MATCH sees the CREATEs of earlier rows
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvM)
            """
        When executing query:
            """
            UNWIND [1, 2, 3] AS i CALL (i) { MATCH (n:WvB), (m:WvM) WITH count(*) AS c CREATE (:WvB) RETURN c } RETURN collect(c) AS cs
            """
        Then the result should be:
            | cs        |
            | [0, 1, 2] |

    Scenario: WHERE in a CALL between two SETs sees the first and not the second
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvC {x: 1, c: 0}), (:WvC {x: 0, c: 0})
            """
        When executing query:
            """
            UNWIND [0, 1] AS i MATCH (b:WvC) SET b.z = i
            CALL (b, i) { WITH b, i WHERE b.z = i AND b.x = i RETURN 1 AS r }
            SET b.x = 1, b.c = b.c + 1
            WITH count(*) AS n MATCH (y:WvC) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs
            """
        Then the result should be, in order:
            | n | cs     |
            | 1 | [0, 1] |

    Scenario: WHEN in a CALL between two SETs sees the first and not the second
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvC {x: 1, c: 0}), (:WvC {x: 0, c: 0})
            """
        When executing query:
            """
            UNWIND [0, 1] AS i MATCH (b:WvC) SET b.z = i
            CALL (b, i) { WHEN b.z = i AND b.x = i THEN RETURN 1 AS r }
            SET b.x = 1, b.c = b.c + 1
            WITH count(*) AS n MATCH (y:WvC) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs
            """
        Then the result should be, in order:
            | n | cs     |
            | 1 | [0, 1] |

    Scenario: WHERE in a CALL between two SETs with WITH * sees the first and not the second
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvC {x: 1, c: 0}), (:WvC {x: 0, c: 0})
            """
        When executing query:
            """
            UNWIND [0, 1] AS i MATCH (b:WvC) SET b.z = i WITH *
            CALL (b, i) { WITH b, i WHERE b.z = i AND b.x = i RETURN 1 AS r }
            SET b.x = 1, b.c = b.c + 1
            WITH count(*) AS n MATCH (y:WvC) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs
            """
        Then the result should be, in order:
            | n | cs     |
            | 1 | [0, 1] |

    Scenario: Read-only conditional CALL before a later SET does not see that SET
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvT {x: 1, c: 0}), (:WvT {x: 0, c: 0})
            """
        When executing query:
            """
            UNWIND [0, 1] AS i MATCH (b:WvT) CALL (b, i) { WHEN b.x = i THEN RETURN 1 AS r }
            SET b.x = 1, b.c = b.c + 1
            WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs
            """
        Then the result should be, in order:
            | n | cs     |
            | 2 | [1, 1] |

    Scenario: WHERE before a later SET does not see that SET
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvT {x: 1, c: 0}), (:WvT {x: 0, c: 0})
            """
        When executing query:
            """
            UNWIND [0, 1] AS i MATCH (b:WvT) WHERE b.x = i
            SET b.x = 1, b.c = b.c + 1
            WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs
            """
        Then the result should be, in order:
            | n | cs     |
            | 2 | [1, 1] |

    Scenario: WHERE before a later SET does not see that SET (indexes :WvT(x))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvT(x)
        And having executed:
            """
            CREATE (:WvT {x: 1, c: 0}), (:WvT {x: 0, c: 0})
            """
        When executing query:
            """
            UNWIND [0, 1] AS i MATCH (b:WvT) WHERE b.x = i
            SET b.x = 1, b.c = b.c + 1
            WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs
            """
        Then the result should be, in order:
            | n | cs     |
            | 2 | [1, 1] |

    Scenario: CALL WITH WHERE before a later SET does not see that SET
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvT {x: 1, c: 0}), (:WvT {x: 0, c: 0})
            """
        When executing query:
            """
            UNWIND [0, 1] AS i MATCH (b:WvT) CALL (b, i) { WITH b, i WHERE b.x = i RETURN 1 AS r }
            SET b.x = 1, b.c = b.c + 1
            WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs
            """
        Then the result should be, in order:
            | n | cs     |
            | 2 | [1, 1] |

    Scenario: Index range scan does not revisit nodes a later CALL moved into the range (indexes :WvA(p))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvA(p)
        And having executed:
            """
            CREATE (:WvA {p: 1}), (:WvA {p: 2})
            """
        When executing query:
            """
            MATCH (n:WvA) WHERE n.p > 0 AND n.p < 15
            CALL (n) { SET n.p = n.p + 10 WITH n MATCH (m:WvA) RETURN count(m) AS k }
            WITH count(*) AS c MATCH (x:WvA) WITH c, x ORDER BY x.p RETURN c, collect(x.p) AS ps
            """
        Then the result should be, in order:
            | c | ps       |
            | 2 | [11, 12] |

    Scenario: Label scan does not revisit nodes a later CALL moved into the range
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvA {p: 1}), (:WvA {p: 2})
            """
        When executing query:
            """
            MATCH (n:WvA) WHERE n.p > 0 AND n.p < 15
            CALL (n) { SET n.p = n.p + 10 WITH n MATCH (m:WvA) RETURN count(m) AS k }
            WITH count(*) AS c MATCH (x:WvA) WITH c, x ORDER BY x.p RETURN c, collect(x.p) AS ps
            """
        Then the result should be, in order:
            | c | ps       |
            | 2 | [11, 12] |

    Scenario: MERGE does not see a later MERGE's write for an earlier row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvG {name: 'A'}), (:WvG {name: 'B'})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MERGE (e:WvG) MERGE (:WvG {name: 'Eve'})
            WITH count(*) AS n MATCH (g:WvG) RETURN n, count(g) AS g
            """
        Then the result should be:
            | n | g |
            | 4 | 3 |

    Scenario: MERGE does not see a later MERGE's write for an earlier row (indexes :WvG(name))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvG(name)
        And having executed:
            """
            CREATE (:WvG {name: 'A'}), (:WvG {name: 'B'})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MERGE (e:WvG) MERGE (:WvG {name: 'Eve'})
            WITH count(*) AS n MATCH (g:WvG) RETURN n, count(g) AS g
            """
        Then the result should be:
            | n | g |
            | 4 | 3 |

    Scenario: MERGE after CREATE sees the CREATEs of every row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i CREATE (:WvK) MERGE (m:WvK)
            WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k
            """
        Then the result should be:
            | n | k |
            | 6 | 3 |

    Scenario: MERGE after CREATE sees the CREATEs of every row (indexes :WvK)
        # Pre-flight: new
        Given an empty graph
        And with new index :WvK
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i CREATE (:WvK) MERGE (m:WvK)
            WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k
            """
        Then the result should be:
            | n | k |
            | 6 | 3 |

    Scenario: MERGE after a one-row CREATE sees it
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            CREATE (:WvK) MERGE (m:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k
            """
        Then the result should be:
            | n | k |
            | 2 | 2 |

    Scenario: MERGE sees its own CREATE for an earlier row
        # Pre-flight: existing
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 1, 2] AS i MERGE (e:WvG {k: i}) WITH count(*) AS n MATCH (g:WvG) RETURN n, count(g) AS g
            """
        Then the result should be:
            | n | g |
            | 3 | 2 |

    Scenario: MERGE sees its own CREATE for an earlier row (indexes :WvG(k))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvG(k)
        When executing query:
            """
            UNWIND [1, 1, 2] AS i MERGE (e:WvG {k: i}) WITH count(*) AS n MATCH (g:WvG) RETURN n, count(g) AS g
            """
        Then the result should be:
            | n | g |
            | 3 | 2 |

    Scenario: MERGE of a relationship sees its own CREATE for an earlier row
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvF {id: 1}), (:WvF {id: 2})
            """
        When executing query:
            """
            UNWIND [1, 2, 3] AS i MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) MERGE (a)-[q:R]->(b)
            WITH count(*) AS n MATCH (:WvF)-[r:R]->() RETURN n, count(r) AS r
            """
        Then the result should be:
            | n | r |
            | 3 | 1 |

    Scenario: MERGE of a relationship sees its own CREATE for an earlier row (indexes EDGE :R)
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :R
            """
        And having executed:
            """
            CREATE (:WvF {id: 1}), (:WvF {id: 2})
            """
        When executing query:
            """
            UNWIND [1, 2, 3] AS i MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) MERGE (a)-[q:R]->(b)
            WITH count(*) AS n MATCH (:WvF)-[r:R]->() RETURN n, count(r) AS r
            """
        Then the result should be:
            | n | r |
            | 3 | 1 |

    Scenario: ORDER BY a non-projected property after SET sorts by the new value
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvJ {id: 1, x: 1, m: -1}), (:WvJ {id: 2, x: 2, m: -1})
            """
        When executing query:
            """
            MATCH (a:WvJ) SET a.x = a.x * a.m RETURN a.id AS id ORDER BY a.x LIMIT 1
            """
        Then the result should be, in order:
            | id |
            | 2  |

    Scenario: ORDER BY a projected but unaliased property after SET sorts by the new value
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvJ {id: 1, x: 1, m: -1}), (:WvJ {id: 2, x: 2, m: -1})
            """
        When executing query:
            """
            MATCH (a:WvJ) SET a.x = a.x * a.m RETURN a.id AS id, a.x ORDER BY a.x LIMIT 1
            """
        Then the result should be, in order:
            | id | a.x |
            | 2  | -2  |

    Scenario: ORDER BY an alias after SET sorts by the new value
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvJ {id: 1, x: 1, m: -1}), (:WvJ {id: 2, x: 2, m: -1})
            """
        When executing query:
            """
            MATCH (a:WvJ) SET a.x = a.x * a.m RETURN a.id AS id, a.x AS x ORDER BY x LIMIT 1
            """
        Then the result should be, in order:
            | id | x  |
            | 2  | -2 |

    Scenario: WITH ORDER BY after SET sorts by the new value
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvJ {id: 1, x: 1, m: -1}), (:WvJ {id: 2, x: 2, m: -1})
            """
        When executing query:
            """
            MATCH (a:WvJ) SET a.x = a.x * a.m WITH a ORDER BY a.x LIMIT 1 RETURN a.id AS id
            """
        Then the result should be, in order:
            | id |
            | 2  |

    Scenario: RETURN after a cross-row SET sees the final value on every row
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvJ {id: 1, x: 1, m: -1}), (:WvJ {id: 2, x: 2, m: -1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (a:WvJ) SET a.x = i RETURN collect(a.x) AS r
            """
        Then the result should be:
            | r            |
            | [2, 2, 2, 2] |

    Scenario: Grouping key after a cross-row SET uses the final value
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvJ {id: 1, x: 1, m: -1}), (:WvJ {id: 2, x: 2, m: -1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (a:WvJ) SET a.x = i RETURN a.x AS x, count(*) AS c ORDER BY x
            """
        Then the result should be, in order:
            | x | c |
            | 2 | 4 |

    Scenario: UNION ALL branch sees the CREATE of an earlier branch
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            CREATE (:WvK) RETURN 1 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x
            """
        Then the result should be:
            | x |
            | 1 |
            | 2 |

    Scenario: UNION ALL branch sees the CREATE of an earlier branch (indexes :WvK)
        # Pre-flight: new
        Given an empty graph
        And with new index :WvK
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            CREATE (:WvK) RETURN 1 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x
            """
        Then the result should be:
            | x |
            | 1 |
            | 2 |

    Scenario: UNION branch sees the CREATE of an earlier branch
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            CREATE (:WvK) RETURN 5 AS x UNION MATCH (n:WvK) RETURN count(n) AS x
            """
        Then the result should be:
            | x |
            | 5 |
            | 2 |

    Scenario: UNION ALL branch sees the SET of an earlier branch
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN 0 AS x UNION ALL MATCH (b:WvS) WHERE b.x = 1 RETURN count(b) AS x
            """
        Then the result should be:
            | x |
            | 0 |
            | 1 |

    Scenario: UNION ALL branch sees the SET of an earlier branch (indexes :WvS(x))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvS(x)
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN 0 AS x UNION ALL MATCH (b:WvS) WHERE b.x = 1 RETURN count(b) AS x
            """
        Then the result should be:
            | x |
            | 0 |
            | 1 |

    Scenario: UNION ALL branch sees the relationship CREATE of an earlier branch
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvF {id: 1}), (:WvF {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) CREATE (a)-[:R]->(b) RETURN 0 AS x
            UNION ALL MATCH (:WvF)-[q:R]->() RETURN count(q) AS x
            """
        Then the result should be:
            | x |
            | 0 |
            | 1 |

    Scenario: UNION ALL branch sees the relationship CREATE of an earlier branch (indexes EDGE :R)
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :R
            """
        And having executed:
            """
            CREATE (:WvF {id: 1}), (:WvF {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) CREATE (a)-[:R]->(b) RETURN 0 AS x
            UNION ALL MATCH (:WvF)-[q:R]->() RETURN count(q) AS x
            """
        Then the result should be:
            | x |
            | 0 |
            | 1 |

    Scenario: UNION ALL branch does not see a node an earlier branch deleted
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 1}), (:WvK {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvK {id: 1}) DELETE a RETURN 0 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x
            """
        Then the result should be:
            | x |
            | 0 |
            | 1 |

    Scenario: Third UNION ALL branch sees the CREATEs of both earlier branches
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            CREATE (:WvK) RETURN 0 AS x UNION ALL CREATE (:WvK) RETURN 0 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x
            """
        Then the result should be:
            | x |
            | 0 |
            | 0 |
            | 3 |

    Scenario: UNION ALL earlier read branch does not see a later CREATE
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            MATCH (n:WvK) RETURN count(n) AS x UNION ALL CREATE (:WvK) RETURN 1 AS x
            """
        Then the result should be:
            | x |
            | 1 |
            | 1 |

    Scenario: SET across two variables reads the earlier assignment
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvW {id: 1, x: 1}), (:WvW {id: 2, x: 2})
            """
        When executing query:
            """
            MATCH (a:WvW {id: 1}), (b:WvW {id: 2}) SET a.x = b.x, b.x = a.x RETURN a.x AS ax, b.x AS bx
            """
        Then the result should be:
            | ax | bx |
            | 2  | 2  |

    Scenario: SET += with a map reads the old values
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvU {x: 1, y: 2})
            """
        When executing query:
            """
            MATCH (a:WvU) SET a += {x: a.y, y: a.x} RETURN a.x AS x, a.y AS y
            """
        Then the result should be:
            | x | y |
            | 2 | 1 |

    Scenario: SET label then WITH then MATCH the label
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS) SET a:WvT WITH a MATCH (b:WvT) RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: SET label then WITH then MATCH the label (indexes :WvT)
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvT
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS) SET a:WvT WITH a MATCH (b:WvT) RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: Cross-row SET property then WITH then MATCH the property
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (a:WvS {id: i}) SET a.x = a.k WITH i MATCH (b:WvS) WHERE b.x = 1 RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: Cross-row SET property then WITH then MATCH the property (indexes :WvS(x))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvS(x)
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (a:WvS {id: i}) SET a.x = a.k WITH i MATCH (b:WvS) WHERE b.x = 1 RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: REMOVE label then WITH then MATCH the label
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) REMOVE a:WvS WITH a MATCH (b:WvS) RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: REMOVE label then WITH then MATCH the label (indexes :WvS)
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvS
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) REMOVE a:WvS WITH a MATCH (b:WvS) RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: REMOVE property then WITH then MATCH on the property
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) REMOVE a.x WITH a MATCH (b:WvS) WHERE b.x IS NOT NULL RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: REMOVE property then WITH then MATCH on the property (indexes :WvS(x))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvS(x)
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) REMOVE a.x WITH a MATCH (b:WvS) WHERE b.x IS NOT NULL RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: DELETE then WITH then MATCH
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 1}), (:WvK {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvK {id: 1}) DELETE a WITH 1 AS d MATCH (b:WvK) RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: DELETE then WITH then MATCH (indexes :WvK(id))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvK(id)
        And having executed:
            """
            CREATE (:WvK {id: 1}), (:WvK {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvK {id: 1}) DELETE a WITH 1 AS d MATCH (b:WvK) RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: DELETE then WITH then OPTIONAL MATCH the deleted node
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 1}), (:WvK {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvK {id: 1}) DELETE a WITH 1 AS d OPTIONAL MATCH (b:WvK {id: 1}) RETURN count(b) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: DELETE then WITH then OPTIONAL MATCH the deleted node (indexes :WvK(id))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvK(id)
        And having executed:
            """
            CREATE (:WvK {id: 1}), (:WvK {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvK {id: 1}) DELETE a WITH 1 AS d OPTIONAL MATCH (b:WvK {id: 1}) RETURN count(b) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: DETACH DELETE then WITH then expand
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvE {id: 1})-[:R]->(:WvE {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvE {id: 1}) DETACH DELETE a WITH 1 AS d MATCH (x:WvE)-[r]-() RETURN count(r) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: DETACH DELETE then WITH then expand (indexes EDGE :R)
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :R
            """
        And having executed:
            """
            CREATE (:WvE {id: 1})-[:R]->(:WvE {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvE {id: 1}) DETACH DELETE a WITH 1 AS d MATCH (x:WvE)-[r]-() RETURN count(r) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: DELETE relationship then WITH then expand
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvE {id: 1})-[:R]->(:WvE {id: 2})
            """
        When executing query:
            """
            MATCH (:WvE {id: 1})-[r:R]->() DELETE r WITH 1 AS d MATCH (x:WvE {id: 2})-[q]-() RETURN count(q) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: DELETE relationship then WITH then expand (indexes EDGE :R)
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :R
            """
        And having executed:
            """
            CREATE (:WvE {id: 1})-[:R]->(:WvE {id: 2})
            """
        When executing query:
            """
            MATCH (:WvE {id: 1})-[r:R]->() DELETE r WITH 1 AS d MATCH (x:WvE {id: 2})-[q]-() RETURN count(q) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: CREATE relationship then WITH then expand
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvF {id: 1}), (:WvF {id: 2})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) CREATE (a)-[:R]->(b) WITH a MATCH (a)-[q:R]->() RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: CREATE relationship then WITH then expand (indexes EDGE :R)
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :R
            """
        And having executed:
            """
            CREATE (:WvF {id: 1}), (:WvF {id: 2})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) CREATE (a)-[:R]->(b) WITH a MATCH (a)-[q:R]->() RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: FOREACH SET then WITH then MATCH on the property
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS) FOREACH (k IN [a.k] | SET a.x = k) WITH a MATCH (b:WvS) WHERE b.x = 1 RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: FOREACH SET then WITH then MATCH on the property (indexes :WvS(x))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvS(x)
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS) FOREACH (k IN [a.k] | SET a.x = k) WITH a MATCH (b:WvS) WHERE b.x = 1 RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: FOREACH CREATE then WITH then MATCH the label
        # Pre-flight: existing
        Given an empty graph
        When executing query:
            """
            UNWIND [1, 2] AS i FOREACH (k IN [1] | CREATE (:WvK)) WITH i MATCH (n:WvK) RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: FOREACH CREATE then WITH then MATCH the label (indexes :WvK)
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvK
        When executing query:
            """
            UNWIND [1, 2] AS i FOREACH (k IN [1] | CREATE (:WvK)) WITH i MATCH (n:WvK) RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: FOREACH body reads its own earlier iteration
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvH {x: 0, s: 1})
            """
        When executing query:
            """
            MATCH (a:WvH) FOREACH (k IN [a.s, a.s, a.s] | SET a.x = a.x + k) RETURN a.x AS x
            """
        Then the result should be:
            | x |
            | 3 |

    Scenario: SET indexed property then WITH then MATCH on it
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvL {id: 1, p: 1, k: 5}), (:WvL {id: 2, p: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvL) SET a.p = a.k WITH a MATCH (b:WvL) WHERE b.p = 5 RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: SET indexed property then WITH then MATCH on it (indexes :WvL(p))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvL(p)
        And having executed:
            """
            CREATE (:WvL {id: 1, p: 1, k: 5}), (:WvL {id: 2, p: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvL) SET a.p = a.k WITH a MATCH (b:WvL) WHERE b.p = 5 RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: SET indexed property then WITH then MATCH on the old value
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvL {id: 1, p: 1, k: 5}), (:WvL {id: 2, p: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvL {id: 1}) SET a.p = a.k WITH a MATCH (b:WvL) WHERE b.p = 1 RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: SET indexed property then WITH then MATCH on the old value (indexes :WvL(p))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvL(p)
        And having executed:
            """
            CREATE (:WvL {id: 1, p: 1, k: 5}), (:WvL {id: 2, p: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvL {id: 1}) SET a.p = a.k WITH a MATCH (b:WvL) WHERE b.p = 1 RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 0 |

    Scenario: MATCH then CREATE of the same label ends
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 1}), (:WvK {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvK) CREATE (:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k
            """
        Then the result should be:
            | n | k |
            | 2 | 4 |

    Scenario: MATCH then CREATE of the same label ends (indexes :WvK)
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvK
        And having executed:
            """
            CREATE (:WvK {id: 1}), (:WvK {id: 2})
            """
        When executing query:
            """
            MATCH (a:WvK) CREATE (:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k
            """
        Then the result should be:
            | n | k |
            | 2 | 4 |

    Scenario: Pattern comprehension expands variable length from a node created in the same part
        # Pre-flight: new
        Given an empty graph
        When executing query:
            """
            CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) RETURN [(a)-[*]->(x) | x.id] AS r
            """
        Then the result should be:
            | r   |
            | [2] |

    Scenario: COUNT expands variable length from a node created in the same part
        # Pre-flight: new
        Given an empty graph
        When executing query:
            """
            CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) RETURN COUNT { (a)-[*]->(x) } AS r
            """
        Then the result should be:
            | r |
            | 1 |

    Scenario: CALL expands variable length from a node created in the same part
        # Pre-flight: new
        Given an empty graph
        When executing query:
            """
            CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) CALL (a) { MATCH (a)-[*]->(x) RETURN x.id AS xid } RETURN xid
            """
        Then the result should be:
            | xid |
            | 2   |

    Scenario: Variable-length MATCH after CREATE and WITH
        # Pre-flight: existing
        Given an empty graph
        When executing query:
            """
            CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) WITH a MATCH (a)-[*]->(x) RETURN x.id AS xid
            """
        Then the result should be:
            | xid |
            | 2   |

    Scenario: WITH * WHERE after SET on an indexed property keeps the filter above the SET (indexes :WvQ(x))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvQ(x)
        And having executed:
            """
            CREATE (:WvQ {x: 1, k: 5}), (:WvQ {x: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvQ) SET a.x = a.k WITH * WHERE a.x = 5
            WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs
            """
        Then the result should be, in order:
            | c | xs     |
            | 2 | [5, 5] |

    Scenario: WITH * WHERE after SET without an index
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvQ {x: 1, k: 5}), (:WvQ {x: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvQ) SET a.x = a.k WITH * WHERE a.x = 5
            WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs
            """
        Then the result should be, in order:
            | c | xs     |
            | 2 | [5, 5] |

    Scenario: WITH a WHERE after SET on an indexed property (indexes :WvQ(x))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvQ(x)
        And having executed:
            """
            CREATE (:WvQ {x: 1, k: 5}), (:WvQ {x: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvQ) SET a.x = a.k WITH a WHERE a.x = 5
            WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs
            """
        Then the result should be, in order:
            | c | xs     |
            | 2 | [5, 5] |

    Scenario: CALL YIELD WHERE after SET sees the new property
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvQ {x: 1, k: 5}), (:WvQ {x: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvQ) SET a.x = a.k CALL mg.procedures() YIELD name WHERE a.x = 5 AND name = 'mg.procedures'
            WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs
            """
        Then the result should be, in order:
            | c | xs     |
            | 2 | [5, 5] |

    Scenario: CALL YIELD WHERE after SET sees the new property (indexes :WvQ(x))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvQ(x)
        And having executed:
            """
            CREATE (:WvQ {x: 1, k: 5}), (:WvQ {x: 2, k: 5})
            """
        When executing query:
            """
            MATCH (a:WvQ) SET a.x = a.k CALL mg.procedures() YIELD name WHERE a.x = 5 AND name = 'mg.procedures'
            WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs
            """
        Then the result should be, in order:
            | c | xs     |
            | 2 | [5, 5] |

    Scenario: WITH * WHERE after SET on an indexed edge property keeps the filter above the SET (indexes EDGE :WvE(w))
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :WvE(w)
            """
        And having executed:
            """
            CREATE (:WvN)-[:WvE {w: 1, k: 5}]->(:WvN)
            """
        When executing query:
            """
            MATCH ()-[r:WvE]->() SET r.w = r.k WITH * WHERE r.w = 5
            WITH count(*) AS c MATCH ()-[q:WvE]->() RETURN c, collect(q.w) AS ws
            """
        Then the result should be:
            | c | ws  |
            | 1 | [5] |

    Scenario: WITH * WHERE after SET on an indexed edge property keeps the filter above the SET (indexes GLOBAL :(w))
        # Pre-flight: new
        Given an empty graph
        And with new edge index :(w)
        And having executed:
            """
            CREATE (:WvN)-[:WvE {w: 1, k: 5}]->(:WvN)
            """
        When executing query:
            """
            MATCH ()-[r:WvE]->() SET r.w = r.k WITH * WHERE r.w = 5
            WITH count(*) AS c MATCH ()-[q:WvE]->() RETURN c, collect(q.w) AS ws
            """
        Then the result should be:
            | c | ws  |
            | 1 | [5] |

    Scenario: WITH * WHERE after SET on an edge property without a property index
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvN)-[:WvE {w: 1, k: 5}]->(:WvN)
            """
        When executing query:
            """
            MATCH ()-[r:WvE]->() SET r.w = r.k WITH * WHERE r.w = 5
            WITH count(*) AS c MATCH ()-[q:WvE]->() RETURN c, collect(q.w) AS ws
            """
        Then the result should be:
            | c | ws  |
            | 1 | [5] |

    Scenario: WITH * WHERE after SET on an edge property without a property index (indexes EDGE :WvE)
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :WvE
            """
        And having executed:
            """
            CREATE (:WvN)-[:WvE {w: 1, k: 5}]->(:WvN)
            """
        When executing query:
            """
            MATCH ()-[r:WvE]->() SET r.w = r.k WITH * WHERE r.w = 5
            WITH count(*) AS c MATCH ()-[q:WvE]->() RETURN c, collect(q.w) AS ws
            """
        Then the result should be:
            | c | ws  |
            | 1 | [5] |

    Scenario: WITH projection does not see a later SET for an earlier row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvR {id: 1, x: 1, t: 10}), (:WvR {id: 2, x: 2, t: 10})
            """
        When executing query:
            """
            MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id WITH a, b, b.x AS v SET a.x = a.t
            WITH a, v ORDER BY a.id RETURN collect(v) AS vs, collect(a.x) AS xs
            """
        Then the result should be, in order:
            | vs     | xs       |
            | [2, 1] | [10, 10] |

    Scenario: SET right-hand side does not see a later SET for an earlier row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvR {id: 1, x: 1, t: 10}), (:WvR {id: 2, x: 2, t: 10})
            """
        When executing query:
            """
            MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id SET a.y = b.x SET a.x = a.t
            WITH a ORDER BY a.id RETURN collect(a.y) AS ys, collect(a.x) AS xs
            """
        Then the result should be, in order:
            | ys     | xs       |
            | [2, 1] | [10, 10] |

    Scenario: CREATE property map does not see a later SET for an earlier row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvR {id: 1, x: 1, t: 10}), (:WvR {id: 2, x: 2, t: 10})
            """
        When executing query:
            """
            MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id CREATE (:WvR2 {v: b.x, id: a.id}) SET a.x = a.t
            WITH count(*) AS n MATCH (c:WvR2) WITH c ORDER BY c.id RETURN collect(c.v) AS vs
            """
        Then the result should be, in order:
            | vs     |
            | [2, 1] |

    Scenario: SET right-hand side does not see a later SET of the same key on another node
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvY {id: 1}), (:WvY {id: 2}), (:WvZ {x: 5, z: 0})
            """
        When executing query:
            """
            MATCH (a:WvY), (b:WvZ) SET a.y = b.x SET b.x = b.z WITH a ORDER BY a.id RETURN collect(a.y) AS ys
            """
        Then the result should be, in order:
            | ys     |
            | [5, 5] |

    Scenario: Pattern comprehension in a SET right-hand side sees an earlier SET
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k SET a.n = size([(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name]) RETURN a.n AS n
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Pattern comprehension in a CREATE property map sees an earlier SET
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k CREATE (m:WvM {c: size([(b:WvP)-[:R]->() WHERE b.flag = 1 | 1])})
            WITH count(*) AS n MATCH (m:WvM) RETURN n, collect(m.c) AS cs
            """
        Then the result should be:
            | n | cs  |
            | 1 | [1] |

    Scenario: Pattern comprehension as a FOREACH list sees an earlier SET
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k FOREACH (v IN [(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name] | CREATE (:WvM {v: v}))
            WITH count(*) AS n OPTIONAL MATCH (m:WvM) RETURN n, collect(m.v) AS vs
            """
        Then the result should be:
            | n | vs    |
            | 1 | ['A'] |

    Scenario: Pattern comprehension in an importing CALL body sees an earlier SET
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvP {name: 'A', k: 1})-[:R]->(:WvP {name: 'B', k: 1})
            """
        When executing query:
            """
            MATCH (a:WvP {name: 'A'}) SET a.flag = a.k CALL (a) { RETURN size([(b:WvP)-[:R]->() WHERE b.flag = 1 | 1]) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 1 |

    Scenario: MERGE property map sees an earlier SET of another row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k MERGE (m:WvM {id: a.id, v: b.x})
            WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs
            """
        Then the result should be, in order:
            | n | vs     |
            | 2 | [1, 1] |

    Scenario: CREATE property map sees an earlier SET of another row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k CREATE (m:WvM {id: a.id, v: b.x})
            WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs
            """
        Then the result should be, in order:
            | n | vs     |
            | 2 | [1, 1] |

    Scenario: FOREACH list sees an earlier SET of another row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k FOREACH (v IN [b.x] | CREATE (:WvM {id: a.id, v: v}))
            WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs
            """
        Then the result should be, in order:
            | n | vs     |
            | 2 | [1, 1] |

    Scenario: SET right-hand side sees an earlier SET of another row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k SET a.v = b.x WITH a ORDER BY a.id RETURN collect(a.v) AS vs
            """
        Then the result should be, in order:
            | vs     |
            | [1, 1] |

    Scenario: CALL body sees an earlier SET of another row through an import
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k CALL (a, b) { RETURN b.x AS v }
            WITH a, v ORDER BY a.id RETURN collect(v) AS vs
            """
        Then the result should be, in order:
            | vs     |
            | [1, 1] |

    Scenario: UNION ALL inside a CALL body sees the CREATE of an earlier leg
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvK {id: 0})
            """
        When executing query:
            """
            CALL () { CREATE (:WvK) RETURN 1 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x } RETURN collect(x) AS xs
            """
        Then the result should be:
            | xs     |
            | [1, 2] |

    Scenario: Two write-read conflicts in one part each see their write
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a.x = a.k
            CALL () { MATCH (b:WvS) WHERE b.x = 1 RETURN count(b) AS c1 }
            SET a.y = c1
            CALL () { MATCH (b:WvS) WHERE b.y = 1 RETURN count(b) AS c2 }
            RETURN c1, c2
            """
        Then the result should be:
            | c1 | c2 |
            | 1  | 1  |

    Scenario: Two write-read conflicts in one part each see their write (indexes :WvS(x))
        # Pre-flight: new
        Given an empty graph
        And with new index :WvS(x)
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a.x = a.k
            CALL () { MATCH (b:WvS) WHERE b.x = 1 RETURN count(b) AS c1 }
            SET a.y = c1
            CALL () { MATCH (b:WvS) WHERE b.y = 1 RETURN count(b) AS c2 }
            RETURN c1, c2
            """
        Then the result should be:
            | c1 | c2 |
            | 1  | 1  |

    Scenario: COUNT with a parameter key after SET sees the new property
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        And parameters are:
            | k | 'x' |
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN COUNT { MATCH (b:WvS) WHERE b[$k] = 1 } AS r
            """
        Then the result should be:
            | r |
            | 1 |

    Scenario: COUNT after SET += a parameter map sees the new property
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        And parameters are:
            | m | {x: 1} |
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a += $m RETURN COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r
            """
        Then the result should be:
            | r |
            | 1 |

    Scenario: COUNT after SET = a parameter map sees the new property
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        And parameters are:
            | m | {x: 1, id: 1} |
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a = $m RETURN COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r
            """
        Then the result should be:
            | r |
            | 1 |

    Scenario: EXISTS after SET of a dynamic label sees the label
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        And parameters are:
            | l | 'WvT' |
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a:$l RETURN EXISTS { MATCH (b:WvT) } AS r
            """
        Then the result should be:
            | r    |
            | true |

    Scenario: COUNT reading properties() after SET sees the new property
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvS {id: 1, x: 0, k: 1}), (:WvS {id: 2, x: 0, k: 1})
            """
        When executing query:
            """
            MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN COUNT { MATCH (b:WvS) WHERE properties(b).x = 1 } AS r
            """
        Then the result should be:
            | r |
            | 1 |

    Scenario: BFS in a CALL body sees an edge created before it
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R]->(:WvV {id: 2})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}) CREATE (s)-[:R]->(:WvV {id: 3}) CALL (s) { MATCH (s)-[*BFS]->(x) RETURN count(x) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: BFS in a CALL body sees an edge created before it (indexes EDGE :R)
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :R
            """
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R]->(:WvV {id: 2})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}) CREATE (s)-[:R]->(:WvV {id: 3}) CALL (s) { MATCH (s)-[*BFS]->(x) RETURN count(x) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: BFS pattern comprehension sees an edge created before it
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R]->(:WvV {id: 2})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}) CREATE (s)-[:R]->(:WvV {id: 3}) RETURN size([(s)-[*BFS]->(x) | x]) AS c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: BFS after CREATE and WITH sees the new edge
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R]->(:WvV {id: 2})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}) CREATE (s)-[:R]->(:WvV {id: 3}) WITH s MATCH (s)-[*BFS]->(x) RETURN count(x) AS c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: WSHORTEST in a CALL body sees an edge created before it
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R {w: 1}]->(:WvV {id: 2})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}) CREATE (s)-[:R {w: 1}]->(:WvV {id: 3}) CALL (s) { MATCH (s)-[*WSHORTEST (r, n | r.w) total]->(x) RETURN count(x) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: WSHORTEST after CREATE and WITH sees the new edge
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R {w: 1}]->(:WvV {id: 2})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}) CREATE (s)-[:R {w: 1}]->(:WvV {id: 3}) WITH s MATCH (s)-[*WSHORTEST (r, n | r.w) total]->(x) RETURN count(x) AS c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: KSHORTEST in a CALL body sees an edge created before it
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R]->(:WvV {id: 2})-[:R]->(:WvV {id: 3})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CREATE (s)-[:R]->(t)
            CALL (s, t) { MATCH p = (s)-[*KSHORTEST]->(t) RETURN collect(size(p)) AS ls } RETURN ls
            """
        Then the result should be:
            | ls     |
            | [1, 2] |

    Scenario: KSHORTEST after CREATE and WITH sees the new edge
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R]->(:WvV {id: 2})-[:R]->(:WvV {id: 3})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CREATE (s)-[:R]->(t)
            WITH s, t MATCH p = (s)-[*KSHORTEST]->(t) RETURN collect(size(p)) AS ls
            """
        Then the result should be:
            | ls     |
            | [1, 2] |

    Scenario: ALLSHORTEST in a CALL body sees an edge created before it
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R {w: 1}]->(:WvV {id: 2})-[:R {w: 1}]->(:WvV {id: 3})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CREATE (s)-[:R {w: 1}]->(t)
            CALL (s, t) { MATCH p = (s)-[*ALLSHORTEST (r, n | r.w) total]->(t) RETURN collect(size(p)) AS ls } RETURN ls
            """
        Then the result should be:
            | ls  |
            | [1] |

    Scenario: ALLSHORTEST after CREATE and WITH sees the new edge
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})-[:R {w: 1}]->(:WvV {id: 2})-[:R {w: 1}]->(:WvV {id: 3})
            """
        When executing query:
            """
            MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CREATE (s)-[:R {w: 1}]->(t)
            WITH s, t MATCH p = (s)-[*ALLSHORTEST (r, n | r.w) total]->(t) RETURN collect(size(p)) AS ls
            """
        Then the result should be:
            | ls  |
            | [1] |

    Scenario: Keyed MERGE import on one label
        # Pre-flight: existing
        Given an empty graph
        When executing query:
            """
            UNWIND [{a: 1, b: 2}, {a: 2, b: 3}, {a: 1, b: 3}, {a: 3, b: 1}] AS r
            MERGE (a:WvL {id: r.a}) MERGE (b:WvL {id: r.b}) MERGE (a)-[:R]->(b)
            WITH count(*) AS n MATCH (x:WvL) WITH n, count(x) AS nodes MATCH (:WvL)-[e:R]->() RETURN n, nodes, count(e) AS edges
            """
        Then the result should be:
            | n | nodes | edges |
            | 4 | 3     | 4     |

    Scenario: Keyed MERGE import on one label (indexes :WvL(id))
        # Pre-flight: existing
        Given an empty graph
        And with new index :WvL(id)
        When executing query:
            """
            UNWIND [{a: 1, b: 2}, {a: 2, b: 3}, {a: 1, b: 3}, {a: 3, b: 1}] AS r
            MERGE (a:WvL {id: r.a}) MERGE (b:WvL {id: r.b}) MERGE (a)-[:R]->(b)
            WITH count(*) AS n MATCH (x:WvL) WITH n, count(x) AS nodes MATCH (:WvL)-[e:R]->() RETURN n, nodes, count(e) AS edges
            """
        Then the result should be:
            | n | nodes | edges |
            | 4 | 3     | 4     |

    Scenario: Keyed MERGE import on two labels
        # Pre-flight: existing
        Given an empty graph
        When executing query:
            """
            UNWIND [{a: 1, b: 2}, {a: 2, b: 3}, {a: 1, b: 3}, {a: 3, b: 1}] AS r
            MERGE (a:WvL1 {id: r.a}) MERGE (b:WvL2 {id: r.b}) MERGE (a)-[:R]->(b)
            WITH count(*) AS n MATCH (x) WHERE x:WvL1 OR x:WvL2 WITH n, count(x) AS nodes MATCH ()-[e:R]->() RETURN n, nodes, count(e) AS edges
            """
        Then the result should be:
            | n | nodes | edges |
            | 4 | 6     | 4     |

    Scenario: CALL body UNWIND reads the list the body set
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvU)
            """
        When executing query:
            """
            UNWIND [1, 2] AS i CALL (i) { MATCH (n:WvU) SET n.l = [i, i] WITH n UNWIND n.l AS v RETURN collect(v) AS vs } RETURN collect(vs) AS r
            """
        Then the result should be:
            | r                |
            | [[1, 1], [2, 2]] |

    Scenario: CALL body variable-length expansion after a body CREATE and WITH *
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i CALL (i) { MATCH (a:WvV {id: 1}) CREATE (a)-[:R]->(:WvV {id: 2}) WITH * MATCH (a)-[*]->(x) RETURN count(x) AS c } RETURN collect(c) AS cs
            """
        Then the result should be:
            | cs     |
            | [1, 2] |

    Scenario: CALL body variable-length expansion after a body CREATE and WITH * (indexes EDGE :R)
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :R
            """
        And having executed:
            """
            CREATE (:WvV {id: 1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i CALL (i) { MATCH (a:WvV {id: 1}) CREATE (a)-[:R]->(:WvV {id: 2}) WITH * MATCH (a)-[*]->(x) RETURN count(x) AS c } RETURN collect(c) AS cs
            """
        Then the result should be:
            | cs     |
            | [1, 2] |

    Scenario: CALL body variable-length expansion after a body CREATE and WITH a
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvV {id: 1})
            """
        When executing query:
            """
            UNWIND [1, 2] AS i CALL (i) { MATCH (a:WvV {id: 1}) CREATE (a)-[:R]->(:WvV {id: 2}) WITH a MATCH (a)-[*]->(x) RETURN count(x) AS c } RETURN collect(c) AS cs
            """
        Then the result should be:
            | cs     |
            | [1, 2] |

    Scenario: Nested read-only CALL body before a later SET does not see that SET
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvT {x: 1, c: 0}), (:WvT {x: 0, c: 0})
            """
        When executing query:
            """
            UNWIND [0, 1] AS i MATCH (b:WvT) CALL (b, i) { CALL (b, i) { WITH b, i WHERE b.x = i RETURN 1 AS r } RETURN r }
            SET b.x = 1, b.c = b.c + 1
            WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs
            """
        Then the result should be, in order:
            | n | cs     |
            | 2 | [1, 1] |

    Scenario: CALL body SET then WITH then variable-length MATCH
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvA4 {id: 1})-[:R]->(:WvM4)-[:R]->(:WvM4)
            """
        When executing query:
            """
            MATCH (n:WvA4) CALL (n) { SET n.x = 1 WITH n MATCH (n)-[*]->(m) RETURN count(m) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 2 |

    Scenario: CALL body CREATE edge then WITH then variable-length MATCH
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvA4 {id: 1})-[:R]->(:WvM4)-[:R]->(:WvM4)
            """
        When executing query:
            """
            MATCH (n:WvA4) CALL (n) { CREATE (n)-[:R]->(k:WvM4) WITH n MATCH (n)-[*]->(m) RETURN count(m) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 3 |

    Scenario: CALL body CREATE edge then WITH then variable-length MATCH (indexes EDGE :R)
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE EDGE INDEX ON :R
            """
        And having executed:
            """
            CREATE (:WvA4 {id: 1})-[:R]->(:WvM4)-[:R]->(:WvM4)
            """
        When executing query:
            """
            MATCH (n:WvA4) CALL (n) { CREATE (n)-[:R]->(k:WvM4) WITH n MATCH (n)-[*]->(m) RETURN count(m) AS c } RETURN c
            """
        Then the result should be:
            | c |
            | 3 |

    Scenario: CALL body CREATE path then WITH then variable-length pattern comprehension
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (:WvA4 {id: 1})-[:R]->(:WvM4)-[:R]->(:WvM4)
            """
        When executing query:
            """
            MATCH (n:WvA4) CALL (n) { CREATE (n)-[:R]->(a:WvQ4)-[:R]->(:WvQ4) WITH a RETURN [(a)-[*]->(x) | 1] AS r } RETURN r
            """
        Then the result should be:
            | r   |
            | [1] |

    Scenario: CREATE property map reads a node created earlier in the same CREATE
        # Deliberate: a CREATE whose property map reads an entity the same CREATE makes is not refused; memgraph answers 1 on master and must keep answering it once root reads go OLD.
        # Pre-flight: existing
        Given an empty graph
        When executing query:
            """
            CREATE (a:WvL {v: 1}), (b:WvL {w: a.v}) RETURN b.w AS w
            """
        Then the result should be:
        | w |
        | 1 |
