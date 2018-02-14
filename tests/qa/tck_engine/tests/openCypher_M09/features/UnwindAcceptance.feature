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

Feature: UnwindAcceptance

  Scenario: Unwinding a list
    Given any graph
    When executing query:
      """
      UNWIND [1, 2, 3] AS x
      RETURN x
      """
    Then the result should be:
      | x |
      | 1 |
      | 2 |
      | 3 |
    And no side effects

  Scenario: Unwinding a range
    Given any graph
    When executing query:
      """
      UNWIND range(1, 3) AS x
      RETURN x
      """
    Then the result should be:
      | x |
      | 1 |
      | 2 |
      | 3 |
    And no side effects

  Scenario: Unwinding a concatenation of lists
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS first, [4, 5, 6] AS second
      UNWIND (first + second) AS x
      RETURN x
      """
    Then the result should be:
      | x |
      | 1 |
      | 2 |
      | 3 |
      | 4 |
      | 5 |
      | 6 |
    And no side effects

  Scenario: Unwinding a collected unwound expression
    Given any graph
    When executing query:
      """
      UNWIND RANGE(1, 2) AS row
      WITH collect(row) AS rows
      UNWIND rows AS x
      RETURN x
      """
    Then the result should be:
      | x |
      | 1 |
      | 2 |
    And no side effects

  Scenario: Unwinding a collected expression
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 1}), ({id: 2})
      """
    When executing query:
      """
      MATCH (row)
      WITH collect(row) AS rows
      UNWIND rows AS node
      RETURN node.id
      """
    Then the result should be:
      | node.id |
      | 1       |
      | 2       |
    And no side effects

  Scenario: Creating nodes from an unwound parameter list
    Given an empty graph
    And having executed:
      """
      CREATE (:Year {year: 2016})
      """
    And parameters are:
      | events | [{year: 2016, id: 1}, {year: 2016, id: 2}] |
    When executing query:
      """
      UNWIND $events AS event
      MATCH (y:Year {year: event.year})
      MERGE (e:Event {id: event.id})
      MERGE (y)<-[:IN]-(e)
      RETURN e.id AS x
      ORDER BY x
      """
    Then the result should be, in order:
      | x |
      | 1 |
      | 2 |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 2 |
      | +labels        | 1 |
      | +properties    | 2 |

  Scenario: Double unwinding a list of lists
    Given any graph
    When executing query:
      """
      WITH [[1, 2, 3], [4, 5, 6]] AS lol
      UNWIND lol AS x
      UNWIND x AS y
      RETURN y
      """
    Then the result should be:
      | y |
      | 1 |
      | 2 |
      | 3 |
      | 4 |
      | 5 |
      | 6 |
    And no side effects

  Scenario: Unwinding the empty list
    Given any graph
    When executing query:
      """
      UNWIND [] AS empty
      RETURN empty
      """
    Then the result should be:
      | empty |
    And no side effects

  Scenario: Unwinding null
    Given any graph
    When executing query:
      """
      UNWIND null AS nil
      RETURN nil
      """
    Then the result should be:
      | nil |
    And no side effects

  Scenario: Unwinding list with duplicates
    Given any graph
    When executing query:
      """
      UNWIND [1, 1, 2, 2, 3, 3, 4, 4, 5, 5] AS duplicate
      RETURN duplicate
      """
    Then the result should be:
      | duplicate |
      | 1         |
      | 1         |
      | 2         |
      | 2         |
      | 3         |
      | 3         |
      | 4         |
      | 4         |
      | 5         |
      | 5         |
    And no side effects

  Scenario: Unwind does not prune context
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      UNWIND list AS x
      RETURN *
      """
    Then the result should be:
      | list      | x |
      | [1, 2, 3] | 1 |
      | [1, 2, 3] | 2 |
      | [1, 2, 3] | 3 |
    And no side effects

  Scenario: Unwind does not remove variables from scope
    Given an empty graph
    And having executed:
      """
      CREATE (s:S),
        (n),
        (e:E),
        (s)-[:X]->(e),
        (s)-[:Y]->(e),
        (n)-[:Y]->(e)
      """
    When executing query:
      """
      MATCH (a:S)-[:X]->(b1)
      WITH a, collect(b1) AS bees
      UNWIND bees AS b2
      MATCH (a)-[:Y]->(b2)
      RETURN a, b2
      """
    Then the result should be:
      | a    | b2   |
      | (:S) | (:E) |
    And no side effects

  Scenario: Multiple unwinds after each other
    Given any graph
    When executing query:
      """
      WITH [1, 2] AS xs, [3, 4] AS ys, [5, 6] AS zs
      UNWIND xs AS x
      UNWIND ys AS y
      UNWIND zs AS z
      RETURN *
      """
    Then the result should be:
      | x | xs     | y | ys     | z | zs     |
      | 1 | [1, 2] | 3 | [3, 4] | 5 | [5, 6] |
      | 1 | [1, 2] | 3 | [3, 4] | 6 | [5, 6] |
      | 1 | [1, 2] | 4 | [3, 4] | 5 | [5, 6] |
      | 1 | [1, 2] | 4 | [3, 4] | 6 | [5, 6] |
      | 2 | [1, 2] | 3 | [3, 4] | 5 | [5, 6] |
      | 2 | [1, 2] | 3 | [3, 4] | 6 | [5, 6] |
      | 2 | [1, 2] | 4 | [3, 4] | 5 | [5, 6] |
      | 2 | [1, 2] | 4 | [3, 4] | 6 | [5, 6] |
    And no side effects

  Scenario: Unwind with merge
    Given an empty graph
    And parameters are:
      | props | [{login: 'login1', name: 'name1'}, {login: 'login2', name: 'name2'}] |
    When executing query:
      """
      UNWIND $props AS prop
      MERGE (p:Person {login: prop.login})
      SET p.name = prop.name
      RETURN p.name, p.login
      """
    Then the result should be:
      | p.name  | p.login  |
      | 'name1' | 'login1' |
      | 'name2' | 'login2' |
    And the side effects should be:
      | +nodes      | 2 |
      | +labels     | 1 |
      | +properties | 4 |
