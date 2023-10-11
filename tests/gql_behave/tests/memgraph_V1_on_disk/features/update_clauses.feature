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
