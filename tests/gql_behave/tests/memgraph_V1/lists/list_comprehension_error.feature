Feature: ListComprehension
  Scenario: Using a list comprehension without expression part
    Given an empty graph
    When executing query:
      """
      RETURN [x in [1, 2, 3] WHERE x = 2] as processed
      """
    Then the result should be:
      | processed |
      | [2]       |

  Scenario: In test1
      When executing query:
          """
          WITH [1, 2, 3, 4] AS l
          RETURN 3 IN l as x
          """
      Then the result should be:
          | x    |
          | true |

  Scenario: In test2
      When executing query:
          """
          WITH [1, '2', 3, 4] AS l
          RETURN 2 IN l as x
          """
      Then the result should be:
          | x     |
          | false |

  Scenario: In test4
      When executing query:
          """
          WITH [1, [2, 3], 4] AS l
          RETURN [3, 2] IN l as x
          """
      Then the result should be:
          | x     |
          | false |
