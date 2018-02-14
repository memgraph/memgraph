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

Feature: UnionAcceptance

  Scenario: Should be able to create text output from union queries
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      MATCH (a:A)
      RETURN a AS a
      UNION
      MATCH (b:B)
      RETURN b AS a
      """
    Then the result should be:
      | a    |
      | (:A) |
      | (:B) |
    And no side effects

  Scenario: Two elements, both unique, not distinct
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS x
      UNION ALL
      RETURN 2 AS x
      """
    Then the result should be:
      | x |
      | 1 |
      | 2 |
    And no side effects

  Scenario: Two elements, both unique, distinct
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS x
      UNION
      RETURN 2 AS x
      """
    Then the result should be:
      | x |
      | 1 |
      | 2 |
    And no side effects

  Scenario: Three elements, two unique, distinct
    Given an empty graph
    When executing query:
      """
      RETURN 2 AS x
      UNION
      RETURN 1 AS x
      UNION
      RETURN 2 AS x
      """
    Then the result should be:
      | x |
      | 2 |
      | 1 |
    And no side effects

  Scenario: Three elements, two unique, not distinct
    Given an empty graph
    When executing query:
      """
      RETURN 2 AS x
      UNION ALL
      RETURN 1 AS x
      UNION ALL
      RETURN 2 AS x
      """
    Then the result should be:
      | x |
      | 2 |
      | 1 |
      | 2 |
    And no side effects
