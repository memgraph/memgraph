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

Feature: OrderByAcceptance

  Background:
    Given an empty graph

  Scenario: ORDER BY should return results in ascending order
    And having executed:
      """
      CREATE (n1 {prop: 1}),
        (n2 {prop: 3}),
        (n3 {prop: -5})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n.prop AS prop
      ORDER BY n.prop
      """
    Then the result should be, in order:
      | prop |
      | -5   |
      | 1    |
      | 3    |
    And no side effects

  Scenario: ORDER BY DESC should return results in descending order
    And having executed:
      """
      CREATE (n1 {prop: 1}),
        (n2 {prop: 3}),
        (n3 {prop: -5})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n.prop AS prop
      ORDER BY n.prop DESC
      """
    Then the result should be, in order:
      | prop |
      | 3    |
      | 1    |
      | -5   |
    And no side effects

  Scenario: ORDER BY of a column introduced in RETURN should return salient results in ascending order
    When executing query:
      """
      WITH [0, 1] AS prows, [[2], [3, 4]] AS qrows
      UNWIND prows AS p
      UNWIND qrows[p] AS q
      WITH p, count(q) AS rng
      RETURN p
      ORDER BY rng
      """
    Then the result should be, in order:
      | p |
      | 0 |
      | 1 |
    And no side effects

  Scenario: Renaming columns before ORDER BY should return results in ascending order
    And having executed:
      """
      CREATE (n1 {prop: 1}),
        (n2 {prop: 3}),
        (n3 {prop: -5})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n.prop AS n
      ORDER BY n + 2
      """
    Then the result should be, in order:
      | n  |
      | -5 |
      | 1  |
      | 3  |
    And no side effects

  Scenario: Handle projections with ORDER BY - GH#4937
    And having executed:
      """
      CREATE (c1:Crew {name: 'Neo', rank: 1}),
        (c2:Crew {name: 'Neo', rank: 2}),
        (c3:Crew {name: 'Neo', rank: 3}),
        (c4:Crew {name: 'Neo', rank: 4}),
        (c5:Crew {name: 'Neo', rank: 5})
      """
    When executing query:
      """
      MATCH (c:Crew {name: 'Neo'})
      WITH c, 0 AS relevance
      RETURN c.rank AS rank
      ORDER BY relevance, c.rank
      """
    Then the result should be, in order:
      | rank |
      | 1    |
      | 2    |
      | 3    |
      | 4    |
      | 5    |
    And no side effects

  Scenario: ORDER BY should order booleans in the expected order
    When executing query:
      """
      UNWIND [true, false] AS bools
      RETURN bools
      ORDER BY bools
      """
    Then the result should be, in order:
      | bools |
      | false |
      | true  |
    And no side effects

  Scenario: ORDER BY DESC should order booleans in the expected order
    When executing query:
      """
      UNWIND [true, false] AS bools
      RETURN bools
      ORDER BY bools DESC
      """
    Then the result should be, in order:
      | bools |
      | true  |
      | false |
    And no side effects

  Scenario: ORDER BY should order strings in the expected order
    When executing query:
      """
      UNWIND ['.*', '', ' ', 'one'] AS strings
      RETURN strings
      ORDER BY strings
      """
    Then the result should be, in order:
      | strings |
      | ''      |
      | ' '     |
      | '.*'    |
      | 'one'   |
    And no side effects

  Scenario: ORDER BY DESC should order strings in the expected order
    When executing query:
      """
      UNWIND ['.*', '', ' ', 'one'] AS strings
      RETURN strings
      ORDER BY strings DESC
      """
    Then the result should be, in order:
      | strings |
      | 'one'   |
      | '.*'    |
      | ' '     |
      | ''      |
    And no side effects

  Scenario: ORDER BY should order ints in the expected order
    When executing query:
      """
      UNWIND [1, 3, 2] AS ints
      RETURN ints
      ORDER BY ints
      """
    Then the result should be, in order:
      | ints |
      | 1    |
      | 2    |
      | 3    |
    And no side effects

  Scenario: ORDER BY DESC should order ints in the expected order
    When executing query:
      """
      UNWIND [1, 3, 2] AS ints
      RETURN ints
      ORDER BY ints DESC
      """
    Then the result should be, in order:
      | ints |
      | 3    |
      | 2    |
      | 1    |
    And no side effects

  Scenario: ORDER BY should order floats in the expected order
    When executing query:
      """
      UNWIND [1.5, 1.3, 999.99] AS floats
      RETURN floats
      ORDER BY floats
      """
    Then the result should be, in order:
      | floats |
      | 1.3    |
      | 1.5    |
      | 999.99 |
    And no side effects

  Scenario: ORDER BY DESC should order floats in the expected order
    When executing query:
      """
      UNWIND [1.5, 1.3, 999.99] AS floats
      RETURN floats
      ORDER BY floats DESC
      """
    Then the result should be, in order:
      | floats |
      | 999.99 |
      | 1.5    |
      | 1.3    |
    And no side effects

  Scenario: Handle ORDER BY with LIMIT 1
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
        (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      ORDER BY p.name
      LIMIT 1
      """
    Then the result should be, in order:
      | name    |
      | 'Craig' |
    And no side effects

  Scenario: ORDER BY with LIMIT 0 should not generate errors
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      ORDER BY p.name
      LIMIT 0
      """
    Then the result should be, in order:
      | name |
    And no side effects

  Scenario: ORDER BY with negative parameter for LIMIT should not generate errors
    And parameters are:
      | limit | -1 |
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      ORDER BY p.name
      LIMIT $`limit`
      """
    Then the result should be, in order:
      | name |
    And no side effects

  Scenario: ORDER BY with a negative LIMIT should fail with a syntax exception
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
        (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      ORDER BY p.name
      LIMIT -1
      """
    Then a SyntaxError should be raised at compile time: NegativeIntegerArgument
