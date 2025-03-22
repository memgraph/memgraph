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

  Scenario: Pattern comprehension inside list comprehension - currently not supported
    Given an empty graph
    And having executed:
      """
      CREATE (n1:X {n: 1}), (m1:Y), (i1:Y), (i2:Y)
      CREATE (n1)-[:T]->(m1),
             (m1)-[:T]->(i1),
             (m1)-[:T]->(i2)
      CREATE (n2:X {n: 2}), (m2), (i3:L), (i4:Y)
      CREATE (n2)-[:T]->(m2),
             (m2)-[:T]->(i3),
             (m2)-[:T]->(i4)
      """
    When executing query:
      """
      MATCH p = (n:X)-->(b)
      RETURN n, [x IN nodes(p) | size([(x)-->(:Y) | 1])] AS list
      """
    Then an error should be raised
