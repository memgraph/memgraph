Feature: Create

    Scenario: Create empty node without clearing database
        When executing query:
            """
            CREATE (n)
            """
        Then the result should be empty

    Scenario: Create node with label without clearing database
        When executing query:
            """
            CREATE (n:L:K)
            """
        Then the result should be empty

    Scenario: Create node with int property without clearing database
        When executing query:
            """
            CREATE (n{a: 1})
            """
        Then the result should be empty

    Scenario: Create node with float property without clearing database
        When executing query:
            """
            CREATE (n{a: 1.0})
            """
        Then the result should be empty

    Scenario: Create node with string property without clearing database
        When executing query:
            """
            CREATE (n{a: 'string'})
            """
        Then the result should be empty

    Scenario: Create node with bool properties without clearing database
        When executing query:
            """
            CREATE (n{a: True, b: false})
            """
        Then the result should be empty

    Scenario: Create node with null property without clearing database
        When executing query:
            """
            CREATE (n{a: NULL})
            """
        Then the result should be empty

    Scenario: Create node with properties without clearing database
        When executing query:
            """
            CREATE (n{a: 1.0, b: false, c: 1, d: 'neki"string"', e: NULL})
            """
        Then the result should be empty

    Scenario: Create node with properties without clearing database
        When executing query:
            """
            CREATE (n:L:K:T {a: 1.0, b: false, c: 1, d: 'neki"string"', e: NULL})
            """
        Then the result should be empty

    Scenario: Create multiple nodes connected by relationships
        When executing query:
            """
            CREATE (a)-[b:X]->(c)<-[d:Y]-(e)
            """
        Then the result should be empty

    Scenario: Create multiple nodes connected by relationships
        When executing query:
            """
            CREATE (n)-[:X]->(n)<-[:Y]-(n)
            """
        Then the result should be empty

    Scenario: Create multiple nodes connected by relationships with properties
        When executing query:
            """
            CREATE (a)-[b:X{a: 1.0}]->(c)<-[d:Y{d: "xyz"}]-(e)
            """
        Then the result should be empty

    Scenario: Create empty node without clearing database:
        When executing query:
            """
            CREATE (n) RETURN n
            """
        Then the result should be:
            | n |
            |( )|

    Scenario: Create node with labels without clearing database and return it
        When executing query:
            """
            CREATE (n:A:B) RETURN n
            """
        Then the result should be:
            |   n  |
            |(:A:B)|

    Scenario: Create node with properties without clearing database and return it
        When executing query:
            """
            CREATE (n{a: 1.0, b: FALSE, c: 1, d: 'neki"string"', e: NULL}) RETURN n
            """
        Then the result should be:
            |                 n                             |
            |({a: 1.0, b: false, c: 1, d: 'neki"string"'})  |

    Scenario: Create node with labels and properties without clearing database and return it
        When executing query:
            """
            CREATE (n:A:B{a: 1.0, b: False, c: 1, d: 'neki"string"', e: NULL}) RETURN n
            """
        Then the result should be:
            |                 n                                 |
            | (:A:B{a: 1.0, b: false, c: 1, d: 'neki"string"'}) |

    Scenario: Create node with properties and labels without clearing database and return its properties
        When executing query:
            """
            CREATE (n:A:B{a: 1.0, b: false, c: 1, d: 'neki"string"', e: NULL}) RETURN n.a, n.b, n.c, n.d, n.e
            """
        Then the result should be:
            | n.a | n.b   | n.c | n.d           | n.e  |
            | 1.0 | false | 1   | 'neki"string"'| null |

    Scenario: Create multiple nodes connected by relationships with properties and return it
        When executing query:
            """
            CREATE (a)-[b:X{a: 1.0}]->(c)<-[d:Y{d: "xyz"}]-(e) RETURN b
            """
        Then the result should be:
            |        b       |
            |  [:X{a: 1.0}]  |

    Scenario: Multiple create 01:
        When executing query:
            """
            CREATE (a)-[b:X{a: 1.0}]->(c)<-[d:Y{d: "xyz"}]-(e) CREATE (n:A:B{a: 1.0, b: False, c: 1, d: 'neki"string"', e: NULL}) RETURN b, n
            """
        Then the result should be:
            |                 n                                 |       b      |
            | (:A:B{a: 1.0, b: false, c: 1, d: 'neki"string"'}) | [:X{a: 1.0}] |

    Scenario: Multiple create 02:
        When executing query:
            """
            CREATE (a)-[:X]->(b) CREATE (a)-[:Y]->(c)
            """
        Then the result should be empty

    Scenario: Multiple create 03:
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B) CREATE (c:C), (a)-[:R]->(b) CREATE (b)-[:R]->(c)
            """
        When executing query:
            """
            MATCH (a)-[]->() MATCH (a:B) MATCH (b:C) RETURN a, b
            """
        Then the result should be:
            | a    | b    |
            | (:B) | (:C) |

    Scenario: Multiple create 04:
        Given an empty graph
        When executing query:
            """
            CREATE (a:A) CREATE (a:B)
            """
        Then an error should be raised
