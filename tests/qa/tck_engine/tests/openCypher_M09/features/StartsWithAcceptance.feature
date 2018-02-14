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

Feature: StartsWithAcceptance

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (:Label {name: 'ABCDEF'}), (:Label {name: 'AB'}),
             (:Label {name: 'abcdef'}), (:Label {name: 'ab'}),
             (:Label {name: ''}), (:Label)
      """

  Scenario: Finding exact matches
    When executing query:
      """
      MATCH (a)
      WHERE a.name STARTS WITH 'ABCDEF'
      RETURN a
      """
    Then the result should be:
      | a                         |
      | (:Label {name: 'ABCDEF'}) |
    And no side effects

  Scenario: Finding beginning of string
    When executing query:
      """
      MATCH (a)
      WHERE a.name STARTS WITH 'ABC'
      RETURN a
      """
    Then the result should be:
      | a                         |
      | (:Label {name: 'ABCDEF'}) |
    And no side effects

  Scenario: Finding end of string 1
    When executing query:
      """
      MATCH (a)
      WHERE a.name ENDS WITH 'DEF'
      RETURN a
      """
    Then the result should be:
      | a                         |
      | (:Label {name: 'ABCDEF'}) |
    And no side effects

  Scenario: Finding end of string 2
    When executing query:
      """
      MATCH (a)
      WHERE a.name ENDS WITH 'AB'
      RETURN a
      """
    Then the result should be:
      | a                     |
      | (:Label {name: 'AB'}) |
    And no side effects

  Scenario: Finding middle of string
    When executing query:
      """
      MATCH (a)
      WHERE a.name STARTS WITH 'a'
        AND a.name ENDS WITH 'f'
      RETURN a
      """
    Then the result should be:
      | a                         |
      | (:Label {name: 'abcdef'}) |
    And no side effects

  Scenario: Finding the empty string
    When executing query:
      """
      MATCH (a)
      WHERE a.name STARTS WITH ''
      RETURN a
      """
    Then the result should be:
      | a                         |
      | (:Label {name: 'ABCDEF'}) |
      | (:Label {name: 'AB'})     |
      | (:Label {name: 'abcdef'}) |
      | (:Label {name: 'ab'})     |
      | (:Label {name: ''})       |
    And no side effects

  Scenario: Finding when the middle is known
    When executing query:
      """
      MATCH (a)
      WHERE a.name CONTAINS 'CD'
      RETURN a
      """
    Then the result should be:
      | a                         |
      | (:Label {name: 'ABCDEF'}) |
    And no side effects

  Scenario: Finding strings starting with whitespace
    And having executed:
      """
      CREATE (:Label {name: ' Foo '}),
             (:Label {name: '\nFoo\n'}),
             (:Label {name: '\tFoo\t'})
      """
    When executing query:
      """
      MATCH (a)
      WHERE a.name STARTS WITH ' '
      RETURN a.name AS name
      """
    Then the result should be:
      | name    |
      | ' Foo ' |
    And no side effects

  Scenario: Finding strings starting with newline
    And having executed:
      """
      CREATE (:Label {name: ' Foo '}),
             (:Label {name: '\nFoo\n'}),
             (:Label {name: '\tFoo\t'})
      """
    When executing query:
      """
      MATCH (a)
      WHERE a.name STARTS WITH '\n'
      RETURN a.name AS name
      """
    Then the result should be:
      | name      |
      | '\nFoo\n' |
    And no side effects

  Scenario: Finding strings ending with newline
    And having executed:
      """
      CREATE (:Label {name: ' Foo '}),
             (:Label {name: '\nFoo\n'}),
             (:Label {name: '\tFoo\t'})
      """
    When executing query:
      """
      MATCH (a)
      WHERE a.name ENDS WITH '\n'
      RETURN a.name AS name
      """
    Then the result should be:
      | name      |
      | '\nFoo\n' |
    And no side effects

  Scenario: Finding strings ending with whitespace
    And having executed:
      """
      CREATE (:Label {name: ' Foo '}),
             (:Label {name: '\nFoo\n'}),
             (:Label {name: '\tFoo\t'})
      """
    When executing query:
      """
      MATCH (a)
      WHERE a.name ENDS WITH ' '
      RETURN a.name AS name
      """
    Then the result should be:
      | name    |
      | ' Foo ' |
    And no side effects

  Scenario: Finding strings containing whitespace
    And having executed:
      """
      CREATE (:Label {name: ' Foo '}),
             (:Label {name: '\nFoo\n'}),
             (:Label {name: '\tFoo\t'})
      """
    When executing query:
      """
      MATCH (a)
      WHERE a.name CONTAINS ' '
      RETURN a.name AS name
      """
    Then the result should be:
      | name    |
      | ' Foo ' |
    And no side effects

  Scenario: Finding strings containing newline
    And having executed:
      """
      CREATE (:Label {name: ' Foo '}),
             (:Label {name: '\nFoo\n'}),
             (:Label {name: '\tFoo\t'})
      """
    When executing query:
      """
      MATCH (a)
      WHERE a.name CONTAINS '\n'
      RETURN a.name AS name
      """
    Then the result should be:
      | name      |
      | '\nFoo\n' |
    And no side effects

  Scenario: No string starts with null
    When executing query:
      """
      MATCH (a)
      WHERE a.name STARTS WITH null
      RETURN a
      """
    Then the result should be:
      | a |
    And no side effects

  Scenario: No string does not start with null
    When executing query:
      """
      MATCH (a)
      WHERE NOT a.name STARTS WITH null
      RETURN a
      """
    Then the result should be:
      | a |
    And no side effects

  Scenario: No string ends with null
    When executing query:
      """
      MATCH (a)
      WHERE a.name ENDS WITH null
      RETURN a
      """
    Then the result should be:
      | a |
    And no side effects

  Scenario: No string does not end with null
    When executing query:
      """
      MATCH (a)
      WHERE NOT a.name ENDS WITH null
      RETURN a
      """
    Then the result should be:
      | a |
    And no side effects

  Scenario: No string contains null
    When executing query:
      """
      MATCH (a)
      WHERE a.name CONTAINS null
      RETURN a
      """
    Then the result should be:
      | a |
    And no side effects

  Scenario: No string does not contain null
    When executing query:
      """
      MATCH (a)
      WHERE NOT a.name CONTAINS null
      RETURN a
      """
    Then the result should be:
      | a |
    And no side effects

  Scenario: Combining string operators
    When executing query:
      """
      MATCH (a)
      WHERE a.name STARTS WITH 'A'
        AND a.name CONTAINS 'C'
        AND a.name ENDS WITH 'EF'
      RETURN a
      """
    Then the result should be:
      | a                         |
      | (:Label {name: 'ABCDEF'}) |
    And no side effects

  Scenario: NOT with CONTAINS
    When executing query:
      """
      MATCH (a)
      WHERE NOT a.name CONTAINS 'b'
      RETURN a
      """
    Then the result should be:
      | a                         |
      | (:Label {name: 'ABCDEF'}) |
      | (:Label {name: 'AB'})     |
      | (:Label {name: ''})       |
    And no side effects

  Scenario: Handling non-string operands for STARTS WITH
    When executing query:
      """
      WITH [1, 3.14, true, [], {}, null] AS operands
      UNWIND operands AS op1
      UNWIND operands AS op2
      WITH op1 STARTS WITH op2 AS v
      RETURN v, count(*)
      """
    Then the result should be:
      | v    | count(*) |
      | null | 36       |
    And no side effects

  Scenario: Handling non-string operands for CONTAINS
    When executing query:
      """
      WITH [1, 3.14, true, [], {}, null] AS operands
      UNWIND operands AS op1
      UNWIND operands AS op2
      WITH op1 STARTS WITH op2 AS v
      RETURN v, count(*)
      """
    Then the result should be:
      | v    | count(*) |
      | null | 36       |
    And no side effects

  Scenario: Handling non-string operands for ENDS WITH
    When executing query:
      """
      WITH [1, 3.14, true, [], {}, null] AS operands
      UNWIND operands AS op1
      UNWIND operands AS op2
      WITH op1 STARTS WITH op2 AS v
      RETURN v, count(*)
      """
    Then the result should be:
      | v    | count(*) |
      | null | 36       |
    And no side effects
