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

Feature: SyntaxErrorAcceptance

  Background:
    Given any graph

  Scenario: Using a nonexistent function
    When executing query:
      """
      MATCH (a)
      RETURN foo(a)
      """
    Then a SyntaxError should be raised at compile time: UnknownFunction

  Scenario: Using `rand()` in aggregations
    When executing query:
      """
      RETURN count(rand())
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: Supplying invalid hexadecimal literal 1
    When executing query:
      """
      RETURN 0x23G34
      """
    Then a SyntaxError should be raised at compile time: InvalidNumberLiteral

  Scenario: Supplying invalid hexadecimal literal 2
    When executing query:
      """
      RETURN 0x23j
      """
    Then a SyntaxError should be raised at compile time: InvalidNumberLiteral
