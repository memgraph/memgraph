Feature: WorkingWithNull

  # Expressions that return null

  Scenario: Missing property returns null
    Given an empty graph
    And having executed:
      """
      CREATE (n:Person {name: 'Alice'})
      """
    When executing query:
      """
      MATCH (n:Person)
      RETURN n.missingProperty AS result
      """
    Then the result should be:
      | result |
      | null   |

  Scenario Outline: Out of bounds list access returns null
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result |
      | null   |

    Examples:
      | expr          |
      | [][0]         |
      | [1, 2, 3][10] |
      | [1, 2, 3][-10]|

  Scenario Outline: Empty list functions return null
    Given any graph
    When executing query:
      """
      RETURN <func>([]) AS result
      """
    Then the result should be:
      | result |
      | null   |

    Examples:
      | func |
      | head |
      | last |

  Scenario Outline: Comparison with null returns null
    Given any graph
    When executing query:
      """
      RETURN 1 <op> null AS result
      """
    Then the result should be:
      | result |
      | null   |

    Examples:
      | op |
      | <  |
      | >  |
      | <= |
      | >= |

  Scenario Outline: Arithmetic with null returns null
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result |
      | null   |

    Examples:
      | expr       |
      | 1 + null   |
      | 1 - null   |
      | 1 * null   |
      | 1 / null   |
      | 1 % null   |
      | 2 ^ null   |

  Scenario Outline: Function with null argument returns null
    Given any graph
    When executing query:
      """
      RETURN <func>(null) AS result
      """
    Then the result should be:
      | result |
      | null   |

    Examples:
      | func      |
      | sin       |
      | cos       |
      | sqrt      |
      | abs       |
      | toInteger |
      | toString  |

  Scenario: Accessing property on null returns null
    Given any graph
    When executing query:
      """
      WITH null AS n
      RETURN n.property AS result
      """
    Then the result should be:
      | result |
      | null   |

  Scenario Outline: Non-null value IS NULL returns false
    Given any graph
    When executing query:
      """
      RETURN <value> IS NULL AS result
      """
    Then the result should be:
      | result |
      | false  |

    Examples:
      | value   |
      | 1       |
      | 'hello' |
      | ''      |
      | []      |

  Scenario Outline: Non-null value IS NOT NULL returns true
    Given any graph
    When executing query:
      """
      RETURN <value> IS NOT NULL AS result
      """
    Then the result should be:
      | result |
      | true   |

    Examples:
      | value   |
      | 1       |
      | 'hello' |

  Scenario: Missing property IS NULL in WHERE clause
    Given an empty graph
    And having executed:
      """
      CREATE (n:Person {name: 'Marko'})
      """
    When executing query:
      """
      MATCH (n:Person)
      WHERE n.age IS NULL
      RETURN n.name AS result
      """
    Then the result should be:
      | result  |
      | 'Marko' |

  Scenario Outline: COALESCE behavior
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result   |
      | <result> |

    Examples:
      | expr                       | result  |
      | coalesce(null, null, 3, 4) | 3       |
      | coalesce(null, null)       | null    |
      | coalesce(1, null, 3)       | 1       |
      | coalesce(42)               | 42      |
      | coalesce(null)             | null    |
      | coalesce(null, 'hello')    | 'hello' |
