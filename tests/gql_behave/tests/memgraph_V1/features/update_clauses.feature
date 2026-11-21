Feature: Update clauses

    Scenario: Match create return test
        Given an empty graph
        And having executed
            """
            CREATE (:x_1), (:z2_), (:qw34)
            """
        When executing query:
            """
            MATCH (a:x_1), (b:z2_), (c:qw34)
            CREATE (a)-[x:X]->(b) CREATE (b)<-[y:Y]-(c)
            RETURN x, y
            """
        Then the result should be:
            |  x   |  y   |
            | [:X] | [:Y] |

    Scenario: Multiple matches in one query
        Given an empty graph
        And having executed
            """
            CREATE (:x{age: 5}), (:y{age: 4}), (:z), (:x), (:y)
            """
        When executing query:
            """
            MATCH (a:x), (b:y), (c:z)
            WHERE a.age=5
            MATCH (b{age: 4})
            RETURN a, b, c
            """
        Then the result should be:
            |  a           |  b           | c    |
            | (:x{age: 5}) | (:y{age: 4}) | (:z) |

    Scenario: Match set one property return test
        Given an empty graph
        And having executed
            """
            CREATE (:q)-[:X]->()
            """
        When executing query:
            """
            MATCH (a:q)-[b]-(c)
            SET a.name='Sinisa'
            RETURN a, b, c
            """
        Then the result should be:
            |  a                   |  b   | c  |
            | (:q{name: 'Sinisa'}) | [:X] | () |

    Scenario: Match set properties from node to node return test
        Given an empty graph
        And having executed
            """
            CREATE (:q{name: 'Sinisa', x: 'y'})-[:X]->({name: 'V',  o: 'Traktor'})
            """
        When executing query:
            """
            MATCH (a:q)-[b]-(c)
            SET c=a
            RETURN a, b, c
            """
        Then the result should be:
            |  a                           |  b   | c                          |
            | (:q{name: 'Sinisa', x: 'y'}) | [:X] | ({name: 'Sinisa', x: 'y'}) |

    Scenario: Match set properties from node to relationship return test
        Given an empty graph
        And having executed
            """
            CREATE (:q{x: 'y'})-[:X]->({y: 't'})
            """
        When executing query:
            """
            MATCH (a:q)-[b]-(c)
            SET b=a
            RETURN a, b, c
            """
        Then the result should be:
            |  a           |  b           | c          |
            | (:q{x: 'y'}) | [:X{x: 'y'}] | ({y: 't'}) |

    Scenario: Match set properties from relationship to node return test
        Given an empty graph
        And having executed
            """
            CREATE (:q)-[:X{x: 'y'}]->({y: 't'})
            """
        When executing query:
            """
            MATCH (a:q)-[b]-(c)
            SET a=b
            RETURN a, b, c
            """
        Then the result should be:
            |  a           |  b           | c          |
            | (:q{x: 'y'}) | [:X{x: 'y'}] | ({y: 't'}) |

    Scenario: Match node set properties without return
        Given an empty graph
        And having executed
            """
            CREATE (n1:Node {test: 1})
            CREATE (n2:Node {test: 2})
            CREATE (n3:Node {test: 3})
            """
        When executing query:
            """
            MATCH (n:Node)
            SET n.test = 4
            """
        Then the result should be empty


    Scenario: Match, set properties from relationship to relationship, return test
        Given an empty graph
        When executing query:
            """
            CREATE ()-[b:X{x: 'y'}]->()-[a:Y]->()
            SET a=b
            RETURN a, b
            """
        Then the result should be:
            |  a           |  b           |
            | [:Y{x: 'y'}] | [:X{x: 'y'}] |

    Scenario: Create, set adding properties, return test
        Given an empty graph
        When executing query:
            """
            CREATE ()-[b:X{x: 'y', y: 'z'}]->()-[a:Y{x: 'z'}]->()
            SET a += b
            RETURN a, b
            """
        Then the result should be:
            |  a                   |  b                   |
            | [:Y{x: 'y', y: 'z'}] | [:X{x: 'y', y: 'z'}] |

    Scenario: Create node and add labels using set, return test
        Given an empty graph
        When executing query:
            """
            CREATE (a)
            SET a :sinisa:vu
            RETURN a
            """
        Then the result should be:
            |  a           |
            | (:sinisa:vu) |

    Scenario: Create node and delete it
        Given an empty graph
        And having executed:
            """
            CREATE (n)
            DELETE (n)
            """
        When executing query:
            """
            MATCH (n)
            RETURN n
            """
        Then the result should be empty

    Scenario: Create node with relationships and delete it, check for relationships
        Given an empty graph
        And having executed:
            """
            CREATE (n)-[:X]->()
            CREATE (n)-[:Y]->()
            DETACH DELETE (n)
            """
        When executing query:
            """
            MATCH ()-[n]->()
            RETURN n
            """
        Then the result should be empty

    Scenario: Create node with relationships and delete it, check for nodes
        Given an empty graph
        And having executed:
            """
            CREATE (n:l{a: 1})-[:X]->()
            CREATE (n)-[:Y]->()
            DETACH DELETE (n)
            """
        When executing query:
            """
            MATCH (n)
            RETURN n
            """
        Then the result should be:
            | n |
            |( )|
            |( )|

    Scenario: Create node with relationships and delete it (without parentheses), check for nodes
        Given an empty graph
        And having executed:
            """
            CREATE (n:l{a: 1})-[:X]->()
            CREATE (n)-[:Y]->()
            DETACH DELETE n
            """
        When executing query:
            """
            MATCH (n)
            RETURN n
            """
        Then the result should be:
            | n |
            |( )|
            |( )|

    Scenario: Set test:
        Given an empty graph
        And having executed:
            """
            CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (a)-[:T]->(b), (b)-[:T]->(c), (c)-[:T]->(a)
            """
        And having executed:
            """
            MATCH (d)--(e) WHERE abs(d.x - e.x)<=1 SET d.x=d.x+2, e.x=e.x+2
            """
        When executing query:
            """
            MATCH(x) RETURN x
            """
        Then the result should be:
            |  x          |
            | (:A{x: 5})  |
            | (:B{x: 10}) |
            | (:C{x: 7})  |

    Scenario: Remove 01
        Given an empty graph
        And having executed
            """
            CREATE (a:A:B:C)
            """
        When executing query:
            """
            MATCH (n) REMOVE n:A:B:C RETURN n
            """
        Then the result should be:
            | n     |
            | ()    |

    Scenario: Remove 02
        Given an empty graph
        And having executed
            """
            CREATE (a:A:B:C)
            """
        When executing query:
            """
            MATCH (n) REMOVE n:B:C RETURN n
            """
        Then the result should be:
            | n       |
            | (:A)    |

    Scenario: Remove 03
        Given an empty graph
        And having executed
            """
            CREATE (a{a: 1, b: 1.0, c: 's', d: false})
            """
        When executing query:
            """
            MATCH (n) REMOVE n:A:B, n.a REMOVE n.b, n.c, n.d RETURN n
            """
        Then the result should be:
            | n     |
            | ()    |

    Scenario: Remove 04
        Given an empty graph
        And having executed
            """
            CREATE (a:A:B{a: 1, b: 's', c: 1.0, d: true})
            """
        When executing query:
            """
            MATCH (n) REMOVE n:B, n.a, n.d RETURN n

            """
        Then the result should be:
            | n                    |
            | (:A{b: 's', c: 1.0}) |

    Scenario: Set a property under a key computed per row
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: 'a'}), (:Item {name: 'b'})
            """
        When executing query:
            """
            MATCH (n)
            SET n[(CASE WHEN n:Person THEN 'userId' ELSE 'id' END)] = n.name
            RETURN n ORDER BY n.name
            """
        Then the result should be, in order:
            | n                                |
            | (:Person {name: 'a', userId: 'a'}) |
            | (:Item {id: 'b', name: 'b'})     |

    Scenario: Set node and relationship properties under variable, parameter and literal keys
        Given an empty graph
        And having executed
            """
            CREATE (:A)-[:X]->(:B)
            """
        And parameters are:
            | key | p |
        When executing query:
            """
            MATCH (a:A)-[r]->()
            UNWIND ['v1', 'v2'] AS k
            SET a[k] = k, r[k] = k, a[$key] = 1, r[$key] = 1, a['l'] = 2, r['l'] = 2
            RETURN DISTINCT a, r
            """
        Then the result should be:
            | a                                    | r                                    |
            | (:A {l: 2, p: 1, v1: 'v1', v2: 'v2'}) | [:X {l: 2, p: 1, v1: 'v1', v2: 'v2'}] |

    Scenario: Set a property under a computed key to null removes it
        Given an empty graph
        And having executed
            """
            CREATE ({a: 1, b: 2})
            """
        When executing query:
            """
            MATCH (n) WITH n, 'a' AS k SET n[k] = null RETURN n
            """
        Then the result should be:
            | n        |
            | ({b: 2}) |

    Scenario: Set a property under a null key fails even on a null target
        Given an empty graph
        When executing query:
            """
            OPTIONAL MATCH (n) SET n[null] = 1
            """
        Then an error should be raised

    Scenario: Set a property under a non-string key fails
        Given an empty graph
        And having executed
            """
            CREATE ()
            """
        When executing query:
            """
            MATCH (n) WITH n, 1 AS k SET n[k] = 1
            """
        Then an error should be raised

    Scenario: Set a property under a computed key on null is a no-op
        Given an empty graph
        When executing query:
            """
            OPTIONAL MATCH (n) SET n['a'] = 1 RETURN n
            """
        Then the result should be:
            | n    |
            | null |

    Scenario: Set a property under a literal key re-evaluates the key on a cached plan
        Given an empty graph
        And having executed
            """
            CREATE ()
            """
        And having executed
            """
            MATCH (n) SET n['a'] = 1
            """
        And having executed
            """
            MATCH (n) SET n['b'] = 2
            """
        When executing query:
            """
            MATCH (n) RETURN n
            """
        Then the result should be:
            | n              |
            | ({a: 1, b: 2}) |

    Scenario: Set properties under computed keys in FOREACH and MERGE
        Given an empty graph
        And having executed
            """
            CREATE ({a: 1, b: 2})
            """
        When executing query:
            """
            MATCH (n) FOREACH (k IN keys(n) | SET n[k + 'Copy'] = n[k])
            WITH n MERGE (m:M) ON CREATE SET m['created'] = true
            RETURN n, m
            """
        Then the result should be:
            | n                                  | m                       |
            | ({a: 1, aCopy: 1, b: 2, bCopy: 2}) | (:M {created: true})    |

    Scenario: Set a property under a computed key on a map fails
        Given an empty graph
        When executing query:
            """
            WITH {a: 1} AS m SET m['x'] = 1
            """
        Then an error should be raised

    Scenario: Explain a set under a computed key
        Given an empty graph
        When executing query:
            """
            EXPLAIN MATCH (n) SET n['a'] = 1
            """
        Then the result should be, in order:
            | QUERY PLAN               |
            | ' * EmptyResult'         |
            | ' * SetPropertyDynamic'  |
            | ' * ScanAll (n)'         |
            | ' * Once'                |
