Feature: ComparabilityEquivalence

  # List and Map Equality

  Scenario Outline: List equality
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result   |
      | <result> |

    Examples:
      | expr                          | result |
      | [1, 2, 3] = [1, 2, 3]         | true   |
      | [1, 2, 3] <> [1, 2, 4]        | true   |
      | [1, 2] <> [1, 2, 3]           | true   |
      | [] = []                       | true   |
      | [[1, 2], [3]] = [[1, 2], [3]] | true   |

  Scenario Outline: List comparison
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result   |
      | <result> |

    Examples:
      | expr                 | result |
      | [1] < [1, 0]         | true   |
      | [1, 5] < [2, 1]      | true   |
      | [1, 2] >= [1, null]  | null   |

  Scenario Outline: Map equality
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result   |
      | <result> |

    Examples:
      | expr                        | result |
      | {a: 1, b: 2} = {a: 1, b: 2} | true   |
      | {a: 1, b: 2} = {b: 2, a: 1} | true   |
      | {a: 1} <> {a: 2}            | true   |
      | {a: 1} <> {b: 1}            | true   |
      | {} = {}                     | true   |

  # Incomparability - Different types

  Scenario Outline: Different types comparison returns null
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result |
      | null   |

    Examples:
      | expr           |
      | 'hello' < 5    |
      | [1, 2] < 'abc' |
      | {a: 1} < [1]   |

  # Incomparability - Duration comparison
  # Per GQL/openCypher spec, durations are incomparable

  Scenario Outline: Duration comparison returns null
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result |
      | null   |

    Examples:
      | expr                                   |
      | duration('P1D') < duration('P2D')      |
      | duration('PT1H') > duration('PT30M')   |
      | duration('P1D') <= duration('P1D')     |
      | duration('P2D') >= duration('P1D')     |

  # Incomparability - NaN ordering comparison
  # NaN is incomparable for ordering operators (<, >, <=, >=)
  # Note: NaN equality follows IEEE 754 (NaN = NaN is false, NaN <> NaN is true)

  Scenario Outline: NaN ordering comparison returns null
    Given any graph
    When executing query:
      """
      RETURN <expr> AS result
      """
    Then the result should be:
      | result |
      | null   |

    Examples:
      | expr              |
      | (0.0/0.0) < 1     |
      | 1 > (0.0/0.0)     |
      | (0.0/0.0) <= 1    |
      | (0.0/0.0) >= 1    |

  # Orderability - ORDER BY with null

  Scenario: ORDER BY places null last in ascending order
    Given any graph
    When executing query:
      """
      UNWIND [1, null, 2] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i    |
      | 1    |
      | 2    |
      | null |

  Scenario: ORDER BY places null first in descending order
    Given any graph
    When executing query:
      """
      UNWIND [1, null, 2] AS i
      RETURN i ORDER BY i DESC
      """
    Then the result should be, in order:
      | i    |
      | null |
      | 2    |
      | 1    |

  Scenario: ORDER BY with multiple nulls
    Given any graph
    When executing query:
      """
      UNWIND [1, null, 2, null, 3] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i    |
      | 1    |
      | 2    |
      | 3    |
      | null |
      | null |

  Scenario: ORDER BY with floats and integers mixed
    Given any graph
    When executing query:
      """
      UNWIND [2.5, 1, 3, 1.5] AS n
      RETURN n ORDER BY n
      """
    Then the result should be, in order:
      | n   |
      | 1   |
      | 1.5 |
      | 2.5 |
      | 3   |

  # Orderability - Cross-type ordering per openCypher spec
  # Ascending order: MAP < NODE < RELATIONSHIP < LIST < PATH < POINT <
  # DATE < LOCAL TIME < LOCAL DATETIME < ZONED DATETIME < DURATION <
  # STRING < BOOLEAN < NUMBER < NULL

  Scenario: ORDER BY with mixed types follows global sort order
    Given any graph
    When executing query:
      """
      UNWIND [1, true, 'hello', 3.14, {a: 1}, [2], null] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i       |
      | {a: 1}  |
      | [2]     |
      | 'hello' |
      | true    |
      | 1       |
      | 3.14    |
      | null    |

  Scenario: ORDER BY DESC with mixed types follows reverse global sort order
    Given any graph
    When executing query:
      """
      UNWIND [1, true, 'hello', 3.14, {a: 1}, [2], null] AS i
      RETURN i ORDER BY i DESC
      """
    Then the result should be, in order:
      | i       |
      | null    |
      | 3.14    |
      | 1       |
      | true    |
      | 'hello' |
      | [2]     |
      | {a: 1}  |

  Scenario: ORDER BY with string and number types
    Given any graph
    When executing query:
      """
      UNWIND ['b', 1, 'a', 2] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i   |
      | 'a' |
      | 'b' |
      | 1   |
      | 2   |

  Scenario: ORDER BY with boolean and number types
    Given any graph
    When executing query:
      """
      UNWIND [true, 1, false, 2] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i     |
      | false |
      | true  |
      | 1     |
      | 2     |

  Scenario: ORDER BY with list and map types
    Given any graph
    When executing query:
      """
      UNWIND [[1], {x: 2}, [3], {y: 4}] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i      |
      | {x: 2} |
      | {y: 4} |
      | [1]    |
      | [3]    |

  Scenario: ORDER BY with empty containers
    Given any graph
    When executing query:
      """
      UNWIND [[], {}, 'text', 42] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i      |
      | {}     |
      | []     |
      | 'text' |
      | 42     |

  Scenario: ORDER BY with multiple values of same type interspersed
    Given any graph
    When executing query:
      """
      UNWIND [3, 'c', 1, 'a', 2, 'b'] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i   |
      | 'a' |
      | 'b' |
      | 'c' |
      | 1   |
      | 2   |
      | 3   |

  Scenario: ORDER BY with nodes and maps
    Given an empty graph
    And having executed:
      """
      CREATE (n:TestNode {val: 1})
      """
    When executing query:
      """
      MATCH (n:TestNode)
      WITH n, {a: 1} AS m
      UNWIND [n, m] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i               |
      | {a: 1}          |
      | (:TestNode {val: 1}) |

  # Orderability - Recursive list ordering per openCypher spec

  Scenario: ORDER BY with lists uses recursive element comparison
    Given any graph
    When executing query:
      """
      UNWIND [[1, 2], [1, 1], [2], [1]] AS l
      RETURN l ORDER BY l
      """
    Then the result should be, in order:
      | l      |
      | [1]    |
      | [1, 1] |
      | [1, 2] |
      | [2]    |

  Scenario: ORDER BY with lists containing mixed types uses orderability
    Given any graph
    When executing query:
      """
      UNWIND [[1, 'foo', 3], [1, 2, 'bar']] AS l
      RETURN l ORDER BY l
      """
    Then the result should be, in order:
      | l             |
      | [1, 'foo', 3] |
      | [1, 2, 'bar'] |

  Scenario: ORDER BY with nested lists
    Given any graph
    When executing query:
      """
      UNWIND [[[2]], [[1]], [[1, 2]]] AS l
      RETURN l ORDER BY l
      """
    Then the result should be, in order:
      | l        |
      | [[1]]    |
      | [[1, 2]] |
      | [[2]]    |

  Scenario: ORDER BY with lists of different lengths
    Given any graph
    When executing query:
      """
      UNWIND [[1, 2, 3], [1, 2], [1]] AS l
      RETURN l ORDER BY l
      """
    Then the result should be, in order:
      | l         |
      | [1]       |
      | [1, 2]    |
      | [1, 2, 3] |

  Scenario: ORDER BY DESC with lists
    Given any graph
    When executing query:
      """
      UNWIND [[1], [1, 2], [2]] AS l
      RETURN l ORDER BY l DESC
      """
    Then the result should be, in order:
      | l      |
      | [2]    |
      | [1, 2] |
      | [1]    |

  # Orderability - Recursive map ordering

  Scenario: ORDER BY with maps uses key comparison
    Given any graph
    When executing query:
      """
      UNWIND [{b: 1}, {a: 1}, {a: 2}] AS m
      RETURN m ORDER BY m
      """
    Then the result should be, in order:
      | m      |
      | {a: 1} |
      | {a: 2} |
      | {b: 1} |

  Scenario: ORDER BY with maps of different sizes
    Given any graph
    When executing query:
      """
      UNWIND [{a: 1, b: 2}, {a: 1}] AS m
      RETURN m ORDER BY m
      """
    Then the result should be, in order:
      | m            |
      | {a: 1}       |
      | {a: 1, b: 2} |

  Scenario: ORDER BY with maps containing mixed value types
    Given any graph
    When executing query:
      """
      UNWIND [{a: 1}, {a: 'x'}] AS m
      RETURN m ORDER BY m
      """
    Then the result should be, in order:
      | m        |
      | {a: 'x'} |
      | {a: 1}   |

  # Equivalence - DISTINCT

  Scenario: DISTINCT removes duplicate integers
    Given any graph
    When executing query:
      """
      UNWIND [1, 2, 1, 3, 2] AS i
      RETURN DISTINCT i
      """
    Then the result should be:
      | i |
      | 1 |
      | 2 |
      | 3 |

  Scenario: DISTINCT treats multiple nulls as equivalent
    Given any graph
    When executing query:
      """
      UNWIND [1, null, 2, null, 3] AS i
      RETURN DISTINCT i
      """
    Then the result should be:
      | i    |
      | 1    |
      | null |
      | 2    |
      | 3    |

  Scenario: DISTINCT with equal lists
    Given any graph
    When executing query:
      """
      UNWIND [[1, 2], [1, 2], [3]] AS l
      RETURN DISTINCT l
      """
    Then the result should be:
      | l      |
      | [1, 2] |
      | [3]    |

  Scenario: DISTINCT with equal maps
    Given any graph
    When executing query:
      """
      UNWIND [{a: 1}, {a: 1}, {b: 2}] AS m
      RETURN DISTINCT m
      """
    Then the result should be:
      | m      |
      | {a: 1} |
      | {b: 2} |

  Scenario: DISTINCT with lists containing null treats them as equivalent
    Given any graph
    When executing query:
      """
      UNWIND [[null], [null]] AS i
      RETURN DISTINCT i
      """
    Then the result should be:
      | i      |
      | [null] |

  # Equivalence - Grouping

  Scenario: GROUP BY treats nulls as equivalent
    Given any graph
    When executing query:
      """
      UNWIND [1, null, 2, null, 1] AS i
      RETURN i, count(*) AS cnt
      """
    Then the result should be:
      | i    | cnt |
      | 1    | 2   |
      | null | 2   |
      | 2    | 1   |

  Scenario: GROUP BY with string keys
    Given any graph
    When executing query:
      """
      UNWIND ['a', 'b', 'a', 'c', 'b'] AS s
      RETURN s, count(*) AS cnt
      """
    Then the result should be:
      | s   | cnt |
      | 'a' | 2   |
      | 'b' | 2   |
      | 'c' | 1   |

  # Aggregation - empty results

  Scenario Outline: Aggregation on empty result
    Given an empty graph
    When executing query:
      """
      MATCH (n:NonExistent)
      RETURN <expr> AS result
      """
    Then the result should be:
      | result   |
      | <result> |

    Examples:
      | expr          | result |
      | count(n)      | 0      |
      | sum(n.value)  | 0      |
      | avg(n.value)  | null   |
      | min(n.value)  | null   |
      | max(n.value)  | null   |
      | collect(n)    | []     |

  # Aggregation - excludes null

  Scenario Outline: Aggregation excludes null values
    Given any graph
    When executing query:
      """
      UNWIND [1, null, 2, null, 3] AS i
      RETURN <expr> AS result
      """
    Then the result should be:
      | result   |
      | <result> |

    Examples:
      | expr       | result    |
      | count(i)   | 3         |
      | count(*)   | 5         |
      | sum(i)     | 6         |
      | avg(i)     | 2.0       |
      | collect(i) | [1, 2, 3] |

  # Node and Relationship Comparability

  Scenario: Same node equals itself
    Given an empty graph
    And having executed:
      """
      CREATE (n:Person {name: 'Alice'})
      """
    When executing query:
      """
      MATCH (n:Person)
      RETURN n = n AS result
      """
    Then the result should be:
      | result |
      | true   |

  Scenario: Different nodes are not equal
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
      """
    When executing query:
      """
      MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
      RETURN a = b AS result
      """
    Then the result should be:
      | result |
      | false  |

  Scenario: Same relationship equals itself
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person)-[r:KNOWS]->(b:Person)
      """
    When executing query:
      """
      MATCH ()-[r:KNOWS]->()
      RETURN r = r AS result
      """
    Then the result should be:
      | result |
      | true   |

  Scenario: Node compared to string returns null
    Given an empty graph
    And having executed:
      """
      CREATE (n:Person)
      """
    When executing query:
      """
      MATCH (n:Person)
      RETURN n = 'string' AS result
      """
    Then the result should be:
      | result |
      | null   |

  Scenario: Relationship compared to integer returns null
    Given an empty graph
    And having executed:
      """
      CREATE (:Person)-[r:KNOWS]->(:Person)
      """
    When executing query:
      """
      MATCH ()-[r:KNOWS]->()
      RETURN r = 123 AS result
      """
    Then the result should be:
      | result |
      | null   |

  # Path Comparability

  Scenario: Same path equals itself
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person)-[:KNOWS]->(b:Person)
      """
    When executing query:
      """
      MATCH p = (:Person)-[:KNOWS]->(:Person)
      RETURN p = p AS result
      """
    Then the result should be:
      | result |
      | true   |

  Scenario: Equal paths are equal
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})
      """
    When executing query:
      """
      MATCH p1 = (a:Person {name: 'A'})-[:KNOWS]->(b:Person)
      MATCH p2 = (a)-[:KNOWS]->(b)
      RETURN p1 = p2 AS result
      """
    Then the result should be:
      | result |
      | true   |

  # Orderability - Temporal types ordering

  Scenario: ORDER BY with temporal types follows hierarchy
    Given any graph
    When executing query:
      """
      WITH date('2024-01-15') AS d,
           localtime('10:30:00') AS lt,
           localdatetime('2024-01-15T10:30:00') AS ldt,
           datetime('2024-01-15T10:30:00Z') AS zdt,
           duration('P1D') AS dur
      UNWIND [d, lt, ldt, zdt, dur] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i                                   |
      | 2024-01-15                          |
      | 10:30:00.000000000                  |
      | 2024-01-15T10:30:00.000000000       |
      | 2024-01-15T10:30:00.000000000+00:00 |
      | P1D                                 |

  Scenario: ORDER BY with point types before temporal
    Given any graph
    When executing query:
      """
      WITH point({x: 1, y: 2}) AS p,
           date('2024-01-15') AS d
      UNWIND [d, p] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i              |
      | POINT(1.0 2.0) |
      | 2024-01-15     |

  Scenario: ORDER BY with temporal types and strings
    Given any graph
    When executing query:
      """
      WITH date('2024-01-15') AS d,
           'hello' AS s
      UNWIND [s, d] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i            |
      | 2024-01-15   |
      | 'hello'      |

  Scenario: ORDER BY with duration and boolean
    Given any graph
    When executing query:
      """
      WITH duration('P1D') AS dur,
           true AS b
      UNWIND [b, dur] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i    |
      | P1D  |
      | true |

  # Orderability - Point types

  Scenario: ORDER BY with 2D points
    Given any graph
    When executing query:
      """
      UNWIND [point({x: 2, y: 1}), point({x: 1, y: 2}), point({x: 1, y: 1})] AS p
      RETURN p ORDER BY p
      """
    Then the result should be, in order:
      | p              |
      | POINT(1.0 1.0) |
      | POINT(1.0 2.0) |
      | POINT(2.0 1.0) |

  # Orderability - Full hierarchy test

  Scenario: ORDER BY with all major types follows complete hierarchy
    Given an empty graph
    And having executed:
      """
      CREATE (n:TestNode {val: 1})-[r:REL]->(m:TestNode {val: 2})
      """
    When executing query:
      """
      MATCH p = (n:TestNode {val: 1})-[r:REL]->(m)
      WITH n, r, p, {a: 1} AS map, [1] AS list,
           point({x: 1, y: 2}) AS pt,
           datetime('2024-01-15T10:30:00Z') AS zdt,
           localdatetime('2024-01-15T10:30:00') AS ldt,
           date('2024-01-15') AS d,
           localtime('10:30:00') AS lt,
           duration('P1D') AS dur,
           'str' AS s, true AS b, 42 AS num, null AS nul
      UNWIND [map, n, r, list, p, pt, zdt, ldt, d, lt, dur, s, b, num, nul] AS i
      RETURN i ORDER BY i
      """
    Then the result should be, in order:
      | i                                                   |
      | {a: 1}                                              |
      | (:TestNode {val: 1})                                |
      | [:REL]                                              |
      | [1]                                                 |
      | <(:TestNode {val: 1})-[:REL]->(:TestNode {val: 2})> |
      | POINT(1.0 2.0)                                      |
      | 2024-01-15                                          |
      | 10:30:00.000000000                                  |
      | 2024-01-15T10:30:00.000000000                       |
      | 2024-01-15T10:30:00.000000000+00:00                 |
      | P1D                                                 |
      | 'str'                                               |
      | true                                                |
      | 42                                                  |
      | null                                                |
