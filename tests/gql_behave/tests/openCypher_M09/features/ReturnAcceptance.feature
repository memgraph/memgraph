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

Feature: ReturnAcceptanceTest

  Scenario: Allow addition
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 1337, version: 99})
      """
    When executing query:
      """
      MATCH (a)
      WHERE a.id = 1337
      RETURN a.version + 5
      """
    Then the result should be:
      | a.version + 5 |
      | 104           |
    And no side effects

  Scenario: Limit to two hits
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 1, 1, 1, 1] AS i
      RETURN i
      LIMIT 2
      """
    Then the result should be:
      | i |
      | 1 |
      | 1 |
    And no side effects

  Scenario: Limit to two hits with explicit order
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'}),
        ({name: 'B'}),
        ({name: 'C'}),
        ({name: 'D'}),
        ({name: 'E'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
      ORDER BY n.name ASC
      LIMIT 2
      """
    Then the result should be:
      | n             |
      | ({name: 'A'}) |
      | ({name: 'B'}) |
    And no side effects

  Scenario: Start the result from the second row
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'}),
        ({name: 'B'}),
        ({name: 'C'}),
        ({name: 'D'}),
        ({name: 'E'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
      ORDER BY n.name ASC
      SKIP 2
      """
    Then the result should be, in order:
      | n             |
      | ({name: 'C'}) |
      | ({name: 'D'}) |
      | ({name: 'E'}) |
    And no side effects

  Scenario: Start the result from the second row by param
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'}),
        ({name: 'B'}),
        ({name: 'C'}),
        ({name: 'D'}),
        ({name: 'E'})
      """
    And parameters are:
      | skipAmount | 2 |
    When executing query:
      """
      MATCH (n)
      RETURN n
      ORDER BY n.name ASC
      SKIP $skipAmount
      """
    Then the result should be, in order:
      | n             |
      | ({name: 'C'}) |
      | ({name: 'D'}) |
      | ({name: 'E'}) |
    And no side effects

  Scenario: Get rows in the middle
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'}),
        ({name: 'B'}),
        ({name: 'C'}),
        ({name: 'D'}),
        ({name: 'E'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
      ORDER BY n.name ASC
      SKIP 2
      LIMIT 2
      """
    Then the result should be, in order:
      | n             |
      | ({name: 'C'}) |
      | ({name: 'D'}) |
    And no side effects

  Scenario: Get rows in the middle by param
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'}),
        ({name: 'B'}),
        ({name: 'C'}),
        ({name: 'D'}),
        ({name: 'E'})
      """
    And parameters are:
      | s | 2 |
      | l | 2 |
    When executing query:
      """
      MATCH (n)
      RETURN n
      ORDER BY n.name ASC
      SKIP $s
      LIMIT $l
      """
    Then the result should be, in order:
      | n             |
      | ({name: 'C'}) |
      | ({name: 'D'}) |
    And no side effects

  Scenario: Sort on aggregated function
    Given an empty graph
    And having executed:
      """
      CREATE ({division: 'A', age: 22}),
        ({division: 'B', age: 33}),
        ({division: 'B', age: 44}),
        ({division: 'C', age: 55})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n.division, max(n.age)
        ORDER BY max(n.age)
      """
    Then the result should be, in order:
      | n.division | max(n.age) |
      | 'A'        | 22         |
      | 'B'        | 44         |
      | 'C'        | 55         |
    And no side effects

  Scenario: Support sort and distinct
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'}),
        ({name: 'B'}),
        ({name: 'C'})
      """
    When executing query:
      """
      MATCH (a)
      RETURN DISTINCT a
        ORDER BY a.name
      """
    Then the result should be, in order:
      | a             |
      | ({name: 'A'}) |
      | ({name: 'B'}) |
      | ({name: 'C'}) |
    And no side effects

  Scenario: Support column renaming
    Given an empty graph
    And having executed:
      """
      CREATE (:Singleton)
      """
    When executing query:
      """
      MATCH (a)
      RETURN a AS ColumnName
      """
    Then the result should be:
      | ColumnName   |
      | (:Singleton) |
    And no side effects

  Scenario: Support ordering by a property after being distinct-ified
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T]->(:B)
      """
    When executing query:
      """
      MATCH (a)-->(b)
      RETURN DISTINCT b
        ORDER BY b.name
      """
    Then the result should be, in order:
      | b    |
      | (:B) |
    And no side effects

  Scenario: Arithmetic precedence test
    Given any graph
    When executing query:
      """
      RETURN 12 / 4 * 3 - 2 * 4
      """
    Then the result should be:
      | 12 / 4 * 3 - 2 * 4 |
      | 1                  |
    And no side effects

  Scenario: Arithmetic precedence with parenthesis test
    Given any graph
    When executing query:
      """
      RETURN 12 / 4 * (3 - 2 * 4)
      """
    Then the result should be:
      | 12 / 4 * (3 - 2 * 4) |
      | -15                  |
    And no side effects

  Scenario: Count star should count everything in scope
    Given an empty graph
    And having executed:
      """
      CREATE (:L1), (:L2), (:L3)
      """
    When executing query:
      """
      MATCH (a)
      RETURN a, count(*)
      ORDER BY count(*)
      """
    Then the result should be:
      | a     | count(*) |
      | (:L1) | 1        |
      | (:L2) | 1        |
      | (:L3) | 1        |
    And no side effects

  Scenario: Absolute function
    Given any graph
    When executing query:
      """
      RETURN abs(-1)
      """
    Then the result should be:
      | abs(-1) |
      | 1       |
    And no side effects

  Scenario: Return collection size
    Given any graph
    When executing query:
      """
      RETURN size([1, 2, 3]) AS n
      """
    Then the result should be:
      | n |
      | 3 |
    And no side effects
