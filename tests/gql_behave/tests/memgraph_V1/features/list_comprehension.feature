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
