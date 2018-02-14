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

Feature: LargeIntegerEquality

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (:Label {id: 4611686018427387905})
      """

  Scenario: Does not lose precision
    When executing query:
      """
      MATCH (p:Label)
      RETURN p.id
      """
    Then the result should be:
      | p.id                |
      | 4611686018427387905 |
    And no side effects

  Scenario: Handling inlined equality of large integer
    When executing query:
      """
      MATCH (p:Label {id: 4611686018427387905})
      RETURN p.id
      """
    Then the result should be:
      | p.id                |
      | 4611686018427387905 |
    And no side effects

  Scenario: Handling explicit equality of large integer
    When executing query:
      """
      MATCH (p:Label)
      WHERE p.id = 4611686018427387905
      RETURN p.id
      """
    Then the result should be:
      | p.id                |
      | 4611686018427387905 |
    And no side effects

  Scenario: Handling inlined equality of large integer, non-equal values
    When executing query:
      """
      MATCH (p:Label {id : 4611686018427387900})
      RETURN p.id
      """
    Then the result should be:
      | p.id                |
    And no side effects

  Scenario: Handling explicit equality of large integer, non-equal values
    When executing query:
      """
      MATCH (p:Label)
      WHERE p.id = 4611686018427387900
      RETURN p.id
      """
    Then the result should be:
      | p.id                |
    And no side effects
