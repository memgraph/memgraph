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

Feature: KeysAcceptance

  Scenario: Using `keys()` on a single node, non-empty result
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'Andres', surname: 'Lopez'})
      """
    When executing query:
      """
      MATCH (n)
      UNWIND keys(n) AS x
      RETURN DISTINCT x AS theProps
      """
    Then the result should be:
      | theProps  |
      | 'name'    |
      | 'surname' |
    And no side effects

  Scenario: Using `keys()` on multiple nodes, non-empty result
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'Andres', surname: 'Lopez'}),
             ({otherName: 'Andres', otherSurname: 'Lopez'})
      """
    When executing query:
      """
      MATCH (n)
      UNWIND keys(n) AS x
      RETURN DISTINCT x AS theProps
      """
    Then the result should be:
      | theProps       |
      | 'name'         |
      | 'surname'      |
      | 'otherName'    |
      | 'otherSurname' |
    And no side effects

  Scenario: Using `keys()` on a single node, empty result
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      UNWIND keys(n) AS x
      RETURN DISTINCT x AS theProps
      """
    Then the result should be:
      | theProps |
    And no side effects

  Scenario: Using `keys()` on an optionally matched node
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      OPTIONAL MATCH (n)
      UNWIND keys(n) AS x
      RETURN DISTINCT x AS theProps
      """
    Then the result should be:
      | theProps |
    And no side effects

  Scenario: Using `keys()` on a relationship, non-empty result
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:KNOWS {level: 'bad', year: '2015'}]->()
      """
    When executing query:
      """
      MATCH ()-[r:KNOWS]-()
      UNWIND keys(r) AS x
      RETURN DISTINCT x AS theProps
      """
    Then the result should be:
      | theProps |
      | 'level'  |
      | 'year'   |
    And no side effects

  Scenario: Using `keys()` on a relationship, empty result
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:KNOWS]->()
      """
    When executing query:
      """
      MATCH ()-[r:KNOWS]-()
      UNWIND keys(r) AS x
      RETURN DISTINCT x AS theProps
      """
    Then the result should be:
      | theProps |
    And no side effects

  Scenario: Using `keys()` on an optionally matched relationship
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:KNOWS]->()
      """
    When executing query:
      """
      OPTIONAL MATCH ()-[r:KNOWS]-()
      UNWIND keys(r) AS x
      RETURN DISTINCT x AS theProps
      """
    Then the result should be:
      | theProps |
    And no side effects

  Scenario: Using `keys()` on a literal map
    Given any graph
    When executing query:
      """
      RETURN keys({name: 'Alice', age: 38, address: {city: 'London', residential: true}}) AS k
      """
    Then the result should be:
      | k                          |
      | ['name', 'age', 'address'] |
    And no side effects

  Scenario: Using `keys()` on a parameter map
    Given any graph
    And parameters are:
      | param | {name: 'Alice', age: 38, address: {city: 'London', residential: true}} |
    When executing query:
      """
      RETURN keys($param) AS k
      """
    Then the result should be (ignoring element order for lists):
      | k                          |
      | ['address', 'name', 'age'] |
    And no side effects
