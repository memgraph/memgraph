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

Feature: Aggregation

  Scenario: `max()` over strings
    Given any graph
    When executing query:
      """
      UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
      RETURN max(i)
      """
    Then the result should be:
      | max(i) |
      | 'b'    |
    And no side effects

  Scenario: `min()` over strings
    Given any graph
    When executing query:
      """
      UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
      RETURN min(i)
      """
    Then the result should be:
      | min(i) |
      | 'B'    |
    And no side effects

  Scenario: `max()` over integers
    Given any graph
    When executing query:
      """
      UNWIND [1, 2, 0, null, -1] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2      |
    And no side effects

  Scenario: `min()` over integers
    Given any graph
    When executing query:
      """
      UNWIND [1, 2, 0, null, -1] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | -1     |
    And no side effects

  Scenario: `max()` over floats
    Given any graph
    When executing query:
      """
      UNWIND [1.0, 2.0, 0.5, null] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2.0    |
    And no side effects

  Scenario: `min()` over floats
    Given any graph
    When executing query:
      """
      UNWIND [1.0, 2.0, 0.5, null] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | 0.5    |
    And no side effects

  Scenario: `max()` over mixed numeric values
    Given any graph
    When executing query:
      """
      UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 5      |
    And no side effects

  Scenario: `min()` over mixed numeric values
    Given any graph
    When executing query:
      """
      UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | 0.1    |
    And no side effects

  Scenario: `max()` over mixed values
    Given any graph
    When executing query:
      """
      UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 1      |
    And no side effects

  Scenario: `min()` over mixed values
    Given any graph
    When executing query:
      """
      UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | [1, 2] |
    And no side effects

  Scenario: `max()` over list values
    Given any graph
    When executing query:
      """
      UNWIND [[1], [2], [2, 1]] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | [2, 1] |
    And no side effects

  Scenario: `min()` over list values
    Given any graph
    When executing query:
      """
      UNWIND [[1], [2], [2, 1]] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | [1]    |
    And no side effects
