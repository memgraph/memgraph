Feature: Unstable 
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

