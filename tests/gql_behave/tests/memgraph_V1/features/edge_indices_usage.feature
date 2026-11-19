Feature: Edge Indices Usage
    Scenario: Scan all by edge type and property equality:
        Given an empty graph
        And with new edge type index :E(p)
        And having executed
            """
            CREATE ()-[:E {p: 1}]->(), ()-[:E {p: 2}]->(), ()-[:E {p: 2}]->(), ()-[:F {p: 2}]->()
            """
        When executing query:
            """
            MATCH ()-[e:E {p: 2}]->() RETURN count(e) AS result
            """
        Then the result should be:
            | result |
            | 2      |

    Scenario: Scan all by edge property range:
        Given an empty graph
        And with new edge index :(p)
        And having executed
            """
            CREATE ()-[:E {p: 10}]->(), ()-[:E {p: 20}]->(), ()-[:F {p: 30}]->()
            """
        When executing query:
            """
            MATCH ()-[e]->() WHERE e.p > 15 RETURN count(e) AS result
            """
        Then the result should be:
            | result |
            | 2      |

    Scenario: Scan all by edge type and property range:
        Given an empty graph
        And with new edge type index :E(p)
        And having executed
            """
            CREATE ()-[:E {p: 10}]->(), ()-[:E {p: 20}]->(), ()-[:F {p: 20}]->()
            """
        When executing query:
            """
            MATCH ()-[e:E]->() WHERE e.p >= 15 RETURN count(e) AS result
            """
        Then the result should be:
            | result |
            | 1      |

    Scenario: An edge-type index does not rebind a node bound by WITH at the pattern's end
        Given an empty graph
        And with new edge type index :SEQUEL
        And having executed
            """
            CREATE (:M {title: 'Matrix'})-[:SEQUEL]->(:M {title: 'JW'}), (:M {title: 'Lone'})
            """
        When executing query:
            """
            MATCH (m:M) WITH m
            MATCH (s)-[:SEQUEL]->(m)
            RETURN m.title AS m, s.title AS s
            """
        Then the result should be:
            | m    | s        |
            | 'JW' | 'Matrix' |

    Scenario: An edge-type index keeps a self-loop pattern to self-loops
        Given an empty graph
        And with new edge type index :T
        And having executed
            """
            CREATE (:A)-[:T]->(:B), (c:C)-[:T]->(c)
            """
        When executing query:
            """
            MATCH (a)-[:T]->(a) RETURN labels(a) AS l
            """
        Then the result should be:
            | l     |
            | ['C'] |
