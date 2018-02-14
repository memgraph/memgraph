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

Feature: SkipLimitAcceptanceTest

  Background:
    Given any graph

  Scenario: SKIP with an expression that depends on variables should fail
    When executing query:
      """
      MATCH (n) RETURN n SKIP n.count
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: LIMIT with an expression that depends on variables should fail
    When executing query:
      """
      MATCH (n) RETURN n LIMIT n.count
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: SKIP with an expression that does not depend on variables
    And having executed:
      """
      UNWIND range(1, 10) AS i
      CREATE ({nr: i})
      """
    When executing query:
      """
      MATCH (n)
      WITH n SKIP toInteger(rand()*9)
      WITH count(*) AS count
      RETURN count > 0 AS nonEmpty
      """
    Then the result should be:
      | nonEmpty |
      | true     |
    And no side effects


  Scenario: LIMIT with an expression that does not depend on variables
    And having executed:
      """
      UNWIND range(1, 3) AS i
      CREATE ({nr: i})
      """
    When executing query:
      """
      MATCH (n)
      WITH n LIMIT toInteger(ceil(1.7))
      RETURN count(*) AS count
      """
    Then the result should be:
      | count |
      | 2     |
    And no side effects
