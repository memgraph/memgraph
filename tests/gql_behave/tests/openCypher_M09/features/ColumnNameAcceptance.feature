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

Feature: ColumnNameAcceptance

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """

  Scenario: Keeping used expression 1
    When executing query:
      """
      MATCH (n)
      RETURN cOuNt( * )
      """
    Then the result should be:
      | cOuNt( * ) |
      | 1          |
    And no side effects

  Scenario: Keeping used expression 2
    When executing query:
      """
      MATCH p = (n)-->(b)
      RETURN nOdEs( p )
      """
    Then the result should be:
      | nOdEs( p ) |
    And no side effects

  Scenario: Keeping used expression 3
    When executing query:
      """
      MATCH p = (n)-->(b)
      RETURN coUnt( dIstInct p )
      """
    Then the result should be:
      | coUnt( dIstInct p ) |
      | 0                   |
    And no side effects

  Scenario: Keeping used expression 4
    When executing query:
      """
      MATCH p = (n)-->(b)
      RETURN aVg(    n.aGe     )
      """
    Then the result should be:
      | aVg(    n.aGe     ) |
      | null                |
    And no side effects
