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

Feature: StartingPointAcceptance

  Scenario: Find all nodes
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'a'}),
             ({name: 'b'}),
             ({name: 'c'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
      """
    Then the result should be:
      | n             |
      | ({name: 'a'}) |
      | ({name: 'b'}) |
      | ({name: 'c'}) |
    And no side effects

  Scenario: Find labelled nodes
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'a'}),
             (:Person),
             (:Animal),
             (:Animal)
      """
    When executing query:
      """
      MATCH (n:Animal)
      RETURN n
      """
    Then the result should be:
      | n         |
      | (:Animal) |
      | (:Animal) |
    And no side effects

  Scenario: Find nodes by property
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 1}),
             ({prop: 2})
      """
    When executing query:
      """
      MATCH (n)
      WHERE n.prop = 2
      RETURN n
      """
    Then the result should be:
      | n           |
      | ({prop: 2}) |
    And no side effects
