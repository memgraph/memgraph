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

Feature: EqualsAcceptance

  Scenario: Number-typed integer comparison
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      WITH collect([0, 0.0]) AS numbers
      UNWIND numbers AS arr
      WITH arr[0] AS expected
      MATCH (n) WHERE toInteger(n.id) = expected
      RETURN n
      """
    Then the result should be:
      | n         |
      | ({id: 0}) |
    And no side effects

  Scenario: Number-typed float comparison
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      WITH collect([0.5, 0]) AS numbers
      UNWIND numbers AS arr
      WITH arr[0] AS expected
      MATCH (n) WHERE toInteger(n.id) = expected
      RETURN n
      """
    Then the result should be:
      | n |
    And no side effects

  Scenario: Any-typed string comparison
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      WITH collect(['0', 0]) AS things
      UNWIND things AS arr
      WITH arr[0] AS expected
      MATCH (n) WHERE toInteger(n.id) = expected
      RETURN n
      """
    Then the result should be:
      | n |
    And no side effects

  Scenario: Comparing nodes to nodes
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (a)
      WITH a
      MATCH (b)
      WHERE a = b
      RETURN count(b)
      """
    Then the result should be:
      | count(b) |
      | 1        |
    And no side effects

  Scenario: Comparing relationships to relationships
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH ()-[a]->()
      WITH a
      MATCH ()-[b]->()
      WHERE a = b
      RETURN count(b)
      """
    Then the result should be:
      | count(b) |
      | 1        |
    And no side effects
