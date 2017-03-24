Feature: Test01

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

