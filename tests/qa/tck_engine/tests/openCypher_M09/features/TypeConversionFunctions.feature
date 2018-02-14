#
# Copyright (c) 2015-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

Feature: TypeConversionFunctions

  Scenario: `toBoolean()` on valid literal string
    Given any graph
    When executing query:
      """
      RETURN toBoolean('true') AS b
      """
    Then the result should be:
      | b    |
      | true |
    And no side effects

  Scenario: `toBoolean()` on booleans
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS b
      RETURN toBoolean(b) AS b
      """
    Then the result should be:
      | b     |
      | true  |
      | false |
    And no side effects

  Scenario: `toBoolean()` on variables with valid string values
    Given any graph
    When executing query:
      """
      UNWIND ['true', 'false'] AS s
      RETURN toBoolean(s) AS b
      """
    Then the result should be:
      | b     |
      | true  |
      | false |
    And no side effects

  Scenario: `toBoolean()` on invalid strings
    Given any graph
    When executing query:
      """
      UNWIND [null, '', ' tru ', 'f alse'] AS things
      RETURN toBoolean(things) AS b
      """
    Then the result should be:
      | b    |
      | null |
      | null |
      | null |
      | null |
    And no side effects

  Scenario Outline: `toBoolean()` on invalid types
    Given any graph
    When executing query:
      """
      WITH [true, <invalid>] AS list
      RETURN toBoolean(list[1]) AS b
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

    Examples:
      | invalid |
      | []      |
      | {}      |
      | 1       |
      | 1.0     |


  Scenario: `toInteger()`
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {age: '42'})
      """
    When executing query:
      """
      MATCH (p:Person { age: '42' })
      WITH *
      MATCH (n)
      RETURN toInteger(n.age) AS age
      """
    Then the result should be:
      | age |
      | 42  |
    And no side effects

  Scenario: `toInteger()` on float
    Given any graph
    When executing query:
      """
      WITH 82.9 AS weight
      RETURN toInteger(weight)
      """
    Then the result should be:
      | toInteger(weight) |
      | 82                |
    And no side effects

  Scenario: `toInteger()` returning null on non-numerical string
    Given any graph
    When executing query:
      """
      WITH 'foo' AS foo_string, '' AS empty_string
      RETURN toInteger(foo_string) AS foo, toInteger(empty_string) AS empty
      """
    Then the result should be:
      | foo  | empty |
      | null | null  |
    And no side effects

  Scenario: `toInteger()` handling mixed number types
    Given any graph
    When executing query:
      """
      WITH [2, 2.9] AS numbers
      RETURN [n IN numbers | toInteger(n)] AS int_numbers
      """
    Then the result should be:
      | int_numbers |
      | [2, 2]      |
    And no side effects

  Scenario: `toInteger()` handling Any type
    Given any graph
    When executing query:
      """
      WITH [2, 2.9, '1.7'] AS things
      RETURN [n IN things | toInteger(n)] AS int_numbers
      """
    Then the result should be:
      | int_numbers |
      | [2, 2, 1]   |
    And no side effects

  Scenario: `toInteger()` on a list of strings
    Given any graph
    When executing query:
      """
      WITH ['2', '2.9', 'foo'] AS numbers
      RETURN [n IN numbers | toInteger(n)] AS int_numbers
      """
    Then the result should be:
      | int_numbers  |
      | [2, 2, null] |
    And no side effects

  Scenario: `toInteger()` on a complex-typed expression
    Given any graph
    And parameters are:
      | param | 1 |
    When executing query:
      """
      RETURN toInteger(1 - $param) AS result
      """
    Then the result should be:
      | result |
      | 0      |
    And no side effects

  Scenario Outline: `toInteger()` failing on invalid arguments
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH p = (n)-[r:T]->()
      RETURN [x IN [1, <invalid>] | toInteger(x) ] AS list
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

    Examples:
      | invalid |
      | true    |
      | []      |
      | {}      |
      | n       |
      | r       |
      | p       |

  Scenario: `toFloat()`
    Given an empty graph
    And having executed:
      """
      CREATE (:Movie {rating: 4})
      """
    When executing query:
      """
      MATCH (m:Movie { rating: 4 })
      WITH *
      MATCH (n)
      RETURN toFloat(n.rating) AS float
      """
    Then the result should be:
      | float |
      | 4.0   |
    And no side effects

  Scenario: `toFloat()` on mixed number types
    Given any graph
    When executing query:
      """
      WITH [3.4, 3] AS numbers
      RETURN [n IN numbers | toFloat(n)] AS float_numbers
      """
    Then the result should be:
      | float_numbers |
      | [3.4, 3.0]    |
    And no side effects

  Scenario: `toFloat()` returning null on non-numerical string
    Given any graph
    When executing query:
      """
      WITH 'foo' AS foo_string, '' AS empty_string
      RETURN toFloat(foo_string) AS foo, toFloat(empty_string) AS empty
      """
    Then the result should be:
      | foo  | empty |
      | null | null  |
    And no side effects

  Scenario: `toFloat()` handling Any type
    Given any graph
    When executing query:
      """
      WITH [3.4, 3, '5'] AS numbers
      RETURN [n IN numbers | toFloat(n)] AS float_numbers
      """
    Then the result should be:
      | float_numbers   |
      | [3.4, 3.0, 5.0] |
    And no side effects

  Scenario: `toFloat()` on a list of strings
    Given any graph
    When executing query:
      """
      WITH ['1', '2', 'foo'] AS numbers
      RETURN [n IN numbers | toFloat(n)] AS float_numbers
      """
    Then the result should be:
      | float_numbers    |
      | [1.0, 2.0, null] |
    And no side effects

  Scenario Outline: `toFloat()` failing on invalid arguments
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH p = (n)-[r:T]->()
      RETURN [x IN [1.0, <invalid>] | toFloat(x) ] AS list
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

    Examples:
      | invalid |
      | true    |
      | []      |
      | {}      |
      | n       |
      | r       |
      | p       |

  Scenario: `toString()`
    Given an empty graph
    And having executed:
      """
      CREATE (:Movie {rating: 4})
      """
    When executing query:
      """
      MATCH (m:Movie { rating: 4 })
      WITH *
      MATCH (n)
      RETURN toString(n.rating)
      """
    Then the result should be:
      | toString(n.rating) |
      | '4'                |
    And no side effects

  Scenario: `toString()` handling boolean properties
    Given an empty graph
    And having executed:
      """
      CREATE (:Movie {watched: true})
      """
    When executing query:
      """
      MATCH (m:Movie)
      RETURN toString(m.watched)
      """
    Then the result should be:
      | toString(m.watched) |
      | 'true'              |
    And no side effects

  Scenario: `toString()` handling inlined boolean
    Given any graph
    When executing query:
      """
      RETURN toString(1 < 0) AS bool
      """
    Then the result should be:
      | bool    |
      | 'false' |
    And no side effects

  Scenario: `toString()` handling boolean literal
    Given any graph
    When executing query:
      """
      RETURN toString(true) AS bool
      """
    Then the result should be:
      | bool   |
      | 'true' |
    And no side effects

  Scenario: `toString()` should work on Any type
    Given any graph
    When executing query:
      """
      RETURN [x IN [1, 2.3, true, 'apa'] | toString(x) ] AS list
      """
    Then the result should be:
      | list                        |
      | ['1', '2.3', 'true', 'apa'] |
    And no side effects

  Scenario: `toString()` on a list of integers
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS numbers
      RETURN [n IN numbers | toString(n)] AS string_numbers
      """
    Then the result should be:
      | string_numbers  |
      | ['1', '2', '3'] |
    And no side effects

  Scenario Outline: `toString()` failing on invalid arguments
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH p = (n)-[r:T]->()
      RETURN [x IN [1, '', <invalid>] | toString(x) ] AS list
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

    Examples:
      | invalid |
      | []      |
      | {}      |
      | n       |
      | r       |
      | p       |

  Scenario: `toString()` should accept potentially correct types 1
    Given any graph
    When executing query:
      """
      UNWIND ['male', 'female', null] AS gen
      RETURN coalesce(toString(gen), 'x') AS result
      """
    Then the result should be:
      | result   |
      | 'male'   |
      | 'female' |
      | 'x'      |
    And no side effects

  Scenario: `toString()` should accept potentially correct types 2
    Given any graph
    When executing query:
      """
      UNWIND ['male', 'female', null] AS gen
      RETURN toString(coalesce(gen, 'x')) AS result
      """
    Then the result should be:
      | result   |
      | 'male'   |
      | 'female' |
      | 'x'      |
    And no side effects
