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

Feature: PathEquality

  Scenario: Direction of traversed relationship is not significant for path equality, simple
    Given an empty graph
    And having executed:
      """
      CREATE (n:A)-[:LOOP]->(n)
      """
    When executing query:
      """
      MATCH p1 = (:A)-->()
      MATCH p2 = (:A)<--()
      RETURN p1 = p2
      """
    Then the result should be:
      | p1 = p2 |
      | true    |
    And no side effects
