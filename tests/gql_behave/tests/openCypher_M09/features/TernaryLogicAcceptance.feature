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

Feature: TernaryLogicAcceptanceTest

  Background:
    Given any graph

  Scenario: The inverse of a null is a null
    When executing query:
      """
      RETURN NOT null AS value
      """
    Then the result should be:
      | value |
      | null  |
    And no side effects

  Scenario: A literal null IS null
    When executing query:
      """
      RETURN null IS NULL AS value
      """
    Then the result should be:
      | value |
      | true  |
    And no side effects

  Scenario: A literal null is not IS NOT null
    When executing query:
      """
      RETURN null IS NOT NULL AS value
      """
    Then the result should be:
      | value |
      | false |
    And no side effects

  Scenario: It is unknown - i.e. null - if a null is equal to a null
    When executing query:
      """
      RETURN null = null AS value
      """
    Then the result should be:
      | value |
      | null  |
    And no side effects

  Scenario: It is unknown - i.e. null - if a null is not equal to a null
    When executing query:
      """
      RETURN null <> null AS value
      """
    Then the result should be:
      | value |
      | null  |
    And no side effects

  Scenario Outline: Using null in AND
    And parameters are:
      | par |  val  |
      | lhs | <lhs> |
      | rhs | <rhs> |
    When executing query:
      """
      RETURN $lhs AND $rhs AS result
      """
    Then the result should be:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | lhs   | rhs   | result |
      | null  | null  | null   |
      | null  | true  | null   |
      | true  | null  | null   |
      | null  | false | false  |
      | false | null  | false  |

  Scenario Outline: Using null in OR
    And parameters are:
      | par |  val  |
      | lhs | <lhs> |
      | rhs | <rhs> |
    When executing query:
      """
      RETURN $lhs OR $rhs AS result
      """
    Then the result should be:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | lhs   | rhs   | result |
      | null  | null  | null   |
      | null  | true  | true   |
      | true  | null  | true   |
      | null  | false | null   |
      | false | null  | null   |

  Scenario Outline: Using null in XOR
    And parameters are:
      | par    |  val     |
      | lhs    | <lhs>    |
      | rhs    | <rhs>    |
    When executing query:
      """
      RETURN $lhs XOR $rhs AS result
      """
    Then the result should be:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | lhs   | rhs   | result |
      | null  | null  | null   |
      | null  | true  | null   |
      | true  | null  | null   |
      | null  | false | null   |
      | false | null  | null   |

  Scenario Outline: Using null in IN
    And parameters are:
      | par    |  val     |
      | elt    | <elt>    |
      | coll   | <coll>   |
    When executing query:
      """
      RETURN $elt IN $coll AS result
      """
    Then the result should be:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | elt  | coll            | result |
      | null | null            | null   |
      | null | [1, 2, 3]       | null   |
      | null | [1, 2, 3, null] | null   |
      | null | []              | false  |
      | 1    | [1, 2, 3, null] | true   |
      | 1    | [null, 1]       | true   |
      | 5    | [1, 2, 3, null] | null   |
