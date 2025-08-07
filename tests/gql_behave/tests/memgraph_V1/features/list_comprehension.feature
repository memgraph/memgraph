Feature: ListComprehension
  Scenario: Returning a list comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH p = (n)-->()
      RETURN [x IN collect(p) | head(nodes(x))] AS p
      """
    Then the result should be:
      | p            |
      | [(:A), (:A)] |

  Scenario: Using a list comprehension in a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH p = (n:A)-->()
      WITH [x IN collect(p) | head(nodes(x))] AS p, count(n) AS c
      RETURN p, c
      """
    Then the result should be:
      | p            | c |
      | [(:A), (:A)] | 2 |

  Scenario: Using a list comprehension in a WHERE
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 'c'})
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)-->(b)
      WHERE n.prop IN [x IN labels(b) | toLower(x)]
      RETURN b
      """
    Then the result should be:
      | b    |
      | (:C) |

  Scenario: Using full list comprehension features
    Given an empty graph
    When executing query:
      """
      RETURN [x in [1, 2, 3] WHERE x = 2| toString(x)] as processed
      """
    Then the result should be:
      | processed |
      | ['2']     |

  Scenario: Using a list comprehension without WHERE part
    Given an empty graph
    When executing query:
      """
      RETURN [x in [1, 2, 3] | toString(x)] as processed
      """
    Then the result should be:
      | processed       |
      | ['1', '2', '3'] |

  Scenario: Using a list comprehension without expression part
    Given an empty graph
    When executing query:
      """
      RETURN [x in [1, 2, 3] WHERE x = 2] as processed
      """
    Then the result should be:
      | processed |
      | [2]       |


  Scenario: Using a list comprehension without WHERE or expression part
    Given an empty graph
    When executing query:
      """
      RETURN [x in [1, 2, 3]] as processed
      """
    Then the result should be:
      | processed |
      | [1, 2, 3] |

  Scenario: Using a list comprehension without WHERE or expression part - nested
    Given an empty graph
    When executing query:
      """
      RETURN [[x in [1, 2, 3]]] as processed
      """
    Then the result should be:
      | processed   |
      | [[1, 2, 3]] |

  Scenario: Using pattern comprehension to test existence
    Given an empty graph
    And having executed:
      """
      CREATE (a:X {prop: 42}), (:X {prop: 43})
      CREATE (a)-[:T]->()
      """
    When executing query:
      """
      MATCH (n:X)
      RETURN n, size([(n)--() | 1]) > 0 AS b
      """
    Then the result should be:
      | n               | b     |
      | (:X {prop: 42}) | true  |
      | (:X {prop: 43}) | false |
    And no side effects

   Scenario: Pattern comprehension in list comprehension - simple
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1}), (b:N {id: 2}), (a)-[:R]->(b)
            """
        When executing query:
            """
            MATCH (a) WHERE single(x in [(a)-[:R]->(b) WHERE b is not null | 1] WHERE true) RETURN a.id AS id
            """
        Then the result should be:
            | id |
            | 1  |

   Scenario: Pattern comprehension in list comprehension - simple reversed
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 1}), (b:N {id: 2}), (a)-[:R]->(b)
            """
        When executing query:
            """
            MATCH (a) WHERE single(x in [(a)-[:R]->(b) WHERE b is null | 1] WHERE true) RETURN a.id AS id
            """
        Then the result should be empty

   Scenario: Pattern comprehension in list comprehension - double nested
        Given an empty graph
        And having executed:
            """
            CREATE (a:A {id: 1})<-[:R1]-(:B)<-[:R2]-(:C)<-[:R3]-(:D {id: 1});
            """
        When executing query:
            """
            MATCH (a:A)
            WHERE single(b IN [(a)<-[:R1]-(b:B) WHERE single(c IN [(b)<-[:R2]-(c:C) WHERE single(d IN [(c)<-[:R3]-(d:D) WHERE d.id = 1 | 1] WHERE true) | 1] WHERE true) | 1] WHERE true)
            RETURN a.id AS id;
            """
        Then the result should be:
            | id |
            | 1  |
