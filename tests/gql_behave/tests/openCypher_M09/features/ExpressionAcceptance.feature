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

Feature: ExpressionAcceptance

  Background:
    Given any graph

  Scenario: IN should work with nested list subscripting
    When executing query:
      """
      WITH [[1, 2, 3]] AS list
      RETURN 3 IN list[0] AS r
      """
    Then the result should be:
      | r    |
      | true |
    And no side effects

  Scenario: IN should work with nested literal list subscripting
    When executing query:
      """
      RETURN 3 IN [[1, 2, 3]][0] AS r
      """
    Then the result should be:
      | r    |
      | true |
    And no side effects

  Scenario: IN should work with list slices
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN 3 IN list[0..1] AS r
      """
    Then the result should be:
      | r     |
      | false |
    And no side effects

  Scenario: IN should work with literal list slices
    When executing query:
      """
      RETURN 3 IN [1, 2, 3][0..1] AS r
      """
    Then the result should be:
      | r     |
      | false |
    And no side effects

  Scenario: Execute n[0]
    When executing query:
      """
      RETURN [1, 2, 3][0] AS value
      """
    Then the result should be:
      | value |
      | 1     |
    And no side effects

  Scenario: Execute n['name'] in read queries
    And having executed:
      """
      CREATE ({name: 'Apa'})
      """
    When executing query:
      """
      MATCH (n {name: 'Apa'})
      RETURN n['nam' + 'e'] AS value
      """
    Then the result should be:
      | value |
      | 'Apa' |
    And no side effects

  Scenario: Execute n['name'] in update queries
    When executing query:
      """
      CREATE (n {name: 'Apa'})
      RETURN n['nam' + 'e'] AS value
      """
    Then the result should be:
      | value |
      | 'Apa' |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Use dynamic property lookup based on parameters when there is no type information
    And parameters are:
      | expr | {name: 'Apa'} |
      | idx  | 'name'        |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[idx] AS value
      """
    Then the result should be:
      | value |
      | 'Apa' |
    And no side effects

  Scenario: Use dynamic property lookup based on parameters when there is lhs type information
    And parameters are:
      | idx | 'name' |
    When executing query:
      """
      CREATE (n {name: 'Apa'})
      RETURN n[$idx] AS value
      """
    Then the result should be:
      | value |
      | 'Apa' |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Use dynamic property lookup based on parameters when there is rhs type information
    And parameters are:
      | expr | {name: 'Apa'} |
      | idx  | 'name'        |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[toString(idx)] AS value
      """
    Then the result should be:
      | value |
      | 'Apa' |
    And no side effects

  Scenario: Use collection lookup based on parameters when there is no type information
    And parameters are:
      | expr | ['Apa'] |
      | idx  | 0       |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[idx] AS value
      """
    Then the result should be:
      | value |
      | 'Apa' |
    And no side effects

  Scenario: Use collection lookup based on parameters when there is lhs type information
    And parameters are:
      | idx | 0 |
    When executing query:
      """
      WITH ['Apa'] AS expr
      RETURN expr[$idx] AS value
      """
    Then the result should be:
      | value |
      | 'Apa' |
    And no side effects

  Scenario: Use collection lookup based on parameters when there is rhs type information
    And parameters are:
      | expr | ['Apa'] |
      | idx  | 0       |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[toInteger(idx)] AS value
      """
    Then the result should be:
      | value |
      | 'Apa' |
    And no side effects

  Scenario: Fail at runtime when attempting to index with an Int into a Map
    And parameters are:
      | expr | {name: 'Apa'} |
      | idx  | 0             |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[idx]
      """
    Then a TypeError should be raised at runtime: MapElementAccessByNonString

  Scenario: Fail at runtime when trying to index into a map with a non-string
    And parameters are:
      | expr | {name: 'Apa'} |
      | idx  | 12.3          |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[idx]
      """
    Then a TypeError should be raised at runtime: MapElementAccessByNonString

  Scenario: Fail at runtime when attempting to index with a String into a Collection
    And parameters are:
      | expr | ['Apa'] |
      | idx  | 'name'  |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[idx]
      """
    Then a TypeError should be raised at runtime: ListElementAccessByNonInteger

  Scenario: Fail at runtime when trying to index into a list with a list
    And parameters are:
      | expr | ['Apa'] |
      | idx  | ['Apa'] |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[idx]
      """
    Then a TypeError should be raised at runtime: ListElementAccessByNonInteger

  Scenario: Fail at compile time when attempting to index with a non-integer into a list
    When executing query:
      """
      WITH [1, 2, 3, 4, 5] AS list, 3.14 AS idx
      RETURN list[idx]
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Fail at runtime when trying to index something which is not a map or collection
    And parameters are:
      | expr | 100 |
      | idx  | 0   |
    When executing query:
      """
      WITH $expr AS expr, $idx AS idx
      RETURN expr[idx]
      """
    Then a TypeError should be raised at runtime: InvalidElementAccess
