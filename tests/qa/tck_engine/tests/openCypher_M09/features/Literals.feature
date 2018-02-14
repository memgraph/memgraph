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

Feature: Literals

  Background:
    Given any graph

  Scenario: Return an integer
    When executing query:
      """
      RETURN 1 AS literal
      """
    Then the result should be:
      | literal |
      | 1       |
    And no side effects

  Scenario: Return a float
    When executing query:
      """
      RETURN 1.0 AS literal
      """
    Then the result should be:
      | literal |
      | 1.0     |
    And no side effects

  Scenario: Return a float in exponent form
    When executing query:
      """
      RETURN -1e-9 AS literal
      """
    Then the result should be:
      | literal     |
      | -.000000001 |
    And no side effects

  Scenario: Return a boolean
    When executing query:
      """
      RETURN true AS literal
      """
    Then the result should be:
      | literal |
      | true    |
    And no side effects

  Scenario: Return a single-quoted string
    When executing query:
      """
      RETURN '' AS literal
      """
    Then the result should be:
      | literal |
      | ''      |
    And no side effects

  Scenario: Return a double-quoted string
    When executing query:
      """
      RETURN "" AS literal
      """
    Then the result should be:
      | literal |
      | ''      |
    And no side effects

  Scenario: Return null
    When executing query:
      """
      RETURN null AS literal
      """
    Then the result should be:
      | literal |
      | null    |
    And no side effects

  Scenario: Return an empty list
    When executing query:
      """
      RETURN [] AS literal
      """
    Then the result should be:
      | literal |
      | []      |
    And no side effects

  Scenario: Return a nonempty list
    When executing query:
      """
      RETURN [0, 1, 2] AS literal
      """
    Then the result should be:
      | literal   |
      | [0, 1, 2] |
    And no side effects

  Scenario: Return an empty map
    When executing query:
      """
      RETURN {} AS literal
      """
    Then the result should be:
      | literal |
      | {}      |
    And no side effects

  Scenario: Return a nonempty map
    When executing query:
      """
      RETURN {k1: 0, k2: 'string'} AS literal
      """
    Then the result should be:
      | literal               |
      | {k1: 0, k2: 'string'} |
    And no side effects
