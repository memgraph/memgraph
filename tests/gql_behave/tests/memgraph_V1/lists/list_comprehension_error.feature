Feature: ListComprehension
  Scenario: Using a list comprehension without expression part
    Given an empty graph
    When executing query:
      """
      RETURN [x in [2] WHERE x = 2] as processed
      """
    Then the result should be:
      | processed |
      | [2]       |

  Scenario: In test2
      When executing query:
          """
          WITH [1, '2', 3, 4] AS l
          RETURN 2 IN l as x
          """
      Then the result should be:
          | x     |
          | false |

