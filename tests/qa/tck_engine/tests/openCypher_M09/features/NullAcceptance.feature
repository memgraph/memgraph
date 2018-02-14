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

Feature: NullAcceptance

  Scenario: Ignore null when setting property
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      SET a.prop = 42
      RETURN a
      """
    Then the result should be:
      | a    |
      | null |
    And no side effects

  Scenario: Ignore null when removing property
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      REMOVE a.prop
      RETURN a
      """
    Then the result should be:
      | a    |
      | null |
    And no side effects

  Scenario: Ignore null when setting properties using an appending map
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      SET a += {prop: 42}
      RETURN a
      """
    Then the result should be:
      | a    |
      | null |
    And no side effects

  Scenario: Ignore null when setting properties using an overriding map
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      SET a = {prop: 42}
      RETURN a
      """
    Then the result should be:
      | a    |
      | null |
    And no side effects

  Scenario: Ignore null when setting label
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      SET a:L
      RETURN a
      """
    Then the result should be:
      | a    |
      | null |
    And no side effects

  Scenario: Ignore null when removing label
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      REMOVE a:L
      RETURN a
      """
    Then the result should be:
      | a    |
      | null |
    And no side effects

  Scenario: Ignore null when deleting node
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      DELETE a
      RETURN a
      """
    Then the result should be:
      | a    |
      | null |
    And no side effects

  Scenario: Ignore null when deleting relationship
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH ()-[r:DoesNotExist]-()
      DELETE r
      RETURN r
      """
    Then the result should be:
      | r    |
      | null |
    And no side effects
