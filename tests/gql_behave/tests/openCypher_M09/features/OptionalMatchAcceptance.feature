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

Feature: OptionalMatchAcceptance

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (s:Single), (a:A {prop: 42}),
             (b:B {prop: 46}), (c:C)
      CREATE (s)-[:REL]->(a),
             (s)-[:REL]->(b),
             (a)-[:REL]->(c),
             (b)-[:LOOP]->(b)
      """

  Scenario: Return null when no matches due to inline label predicate
    When executing query:
      """
      MATCH (n:Single)
      OPTIONAL MATCH (n)-[r]-(m:NonExistent)
      RETURN r
      """
    Then the result should be:
      | r    |
      | null |
    And no side effects

  Scenario: Return null when no matches due to label predicate in WHERE
    When executing query:
      """
      MATCH (n:Single)
      OPTIONAL MATCH (n)-[r]-(m)
      WHERE m:NonExistent
      RETURN r
      """
    Then the result should be:
      | r    |
      | null |
    And no side effects

  Scenario: Respect predicates on the OPTIONAL MATCH
    When executing query:
      """
      MATCH (n:Single)
      OPTIONAL MATCH (n)-[r]-(m)
      WHERE m.prop = 42
      RETURN m
      """
    Then the result should be:
      | m               |
      | (:A {prop: 42}) |
    And no side effects

  Scenario: Returning label predicate on null node
    When executing query:
      """
      MATCH (n:Single)
      OPTIONAL MATCH (n)-[r:TYPE]-(m)
      RETURN m:TYPE
      """
    Then the result should be:
      | m:TYPE |
      | null   |
    And no side effects

  Scenario: MATCH after OPTIONAL MATCH
    When executing query:
      """
      MATCH (a:Single)
      OPTIONAL MATCH (a)-->(b:NonExistent)
      OPTIONAL MATCH (a)-->(c:NonExistent)
      WITH coalesce(b, c) AS x
      MATCH (x)-->(d)
      RETURN d
      """
    Then the result should be:
      | d |
    And no side effects

  Scenario: WITH after OPTIONAL MATCH
    When executing query:
      """
      OPTIONAL MATCH (a:A)
      WITH a AS a
      MATCH (b:B)
      RETURN a, b
      """
    Then the result should be:
      | a               | b               |
      | (:A {prop: 42}) | (:B {prop: 46}) |
    And no side effects

  Scenario: Named paths in optional matches
    When executing query:
      """
      MATCH (a:A)
      OPTIONAL MATCH p = (a)-[:X]->(b)
      RETURN p
      """
    Then the result should be:
      | p    |
      | null |
    And no side effects

  Scenario: OPTIONAL MATCH and bound nodes
    When executing query:
      """
      MATCH (a:A), (b:C)
      OPTIONAL MATCH (x)-->(b)
      RETURN x
      """
    Then the result should be:
      | x               |
      | (:A {prop: 42}) |
    And no side effects

  Scenario: OPTIONAL MATCH with labels on the optional end node
    And having executed:
      """
      CREATE (:X), (x:X), (y1:Y), (y2:Y:Z)
      CREATE (x)-[:REL]->(y1),
             (x)-[:REL]->(y2)
      """
    When executing query:
      """
      MATCH (a:X)
      OPTIONAL MATCH (a)-->(b:Y)
      RETURN b
      """
    Then the result should be:
      | b      |
      | null   |
      | (:Y)   |
      | (:Y:Z) |
    And no side effects

  Scenario: Named paths inside optional matches with node predicates
    When executing query:
      """
      MATCH (a:A), (b:B)
      OPTIONAL MATCH p = (a)-[:X]->(b)
      RETURN p
      """
    Then the result should be:
      | p    |
      | null |
    And no side effects

  Scenario: Variable length optional relationships
    When executing query:
      """
      MATCH (a:Single)
      OPTIONAL MATCH (a)-[*]->(b)
      RETURN b
      """
    Then the result should be:
      | b               |
      | (:A {prop: 42}) |
      | (:B {prop: 46}) |
      | (:B {prop: 46}) |
      | (:C)            |
    And no side effects

  Scenario: Variable length optional relationships with length predicates
    When executing query:
      """
      MATCH (a:Single)
      OPTIONAL MATCH (a)-[*3..]-(b)
      RETURN b
      """
    Then the result should be:
      | b    |
      | null |
    And no side effects

  Scenario: Optionally matching self-loops
    When executing query:
      """
      MATCH (a:B)
      OPTIONAL MATCH (a)-[r]-(a)
      RETURN r
      """
    Then the result should be:
      | r       |
      | [:LOOP] |
    And no side effects

  Scenario: Optionally matching self-loops without matches
    When executing query:
      """
      MATCH (a)
      WHERE NOT (a:B)
      OPTIONAL MATCH (a)-[r]->(a)
      RETURN r
      """
    Then the result should be:
      | r    |
      | null |
      | null |
      | null |
    And no side effects

  Scenario: Variable length optional relationships with bound nodes
    When executing query:
      """
      MATCH (a:Single), (x:C)
      OPTIONAL MATCH (a)-[*]->(x)
      RETURN x
      """
    Then the result should be:
      | x    |
      | (:C) |
    And no side effects

  Scenario: Variable length optional relationships with bound nodes, no matches
    When executing query:
      """
      MATCH (a:A), (b:B)
      OPTIONAL MATCH p = (a)-[*]->(b)
      RETURN p
      """
    Then the result should be:
      | p    |
      | null |
    And no side effects

  Scenario: Longer pattern with bound nodes
    When executing query:
      """
      MATCH (a:Single), (c:C)
      OPTIONAL MATCH (a)-->(b)-->(c)
      RETURN b
      """
    Then the result should be:
      | b               |
      | (:A {prop: 42}) |
    And no side effects

  Scenario: Longer pattern with bound nodes without matches
    When executing query:
      """
      MATCH (a:A), (c:C)
      OPTIONAL MATCH (a)-->(b)-->(c)
      RETURN b
      """
    Then the result should be:
      | b    |
      | null |
    And no side effects

  Scenario: Handling correlated optional matches; first does not match implies second does not match
    When executing query:
      """
      MATCH (a:A), (b:B)
      OPTIONAL MATCH (a)-->(x)
      OPTIONAL MATCH (x)-[r]->(b)
      RETURN x, r
      """
    Then the result should be:
      | x    | r    |
      | (:C) | null |
    And no side effects

  Scenario: Handling optional matches between optionally matched entities
    When executing query:
      """
      OPTIONAL MATCH (a:NotThere)
      WITH a
      MATCH (b:B)
      WITH a, b
      OPTIONAL MATCH (b)-[r:NOR_THIS]->(a)
      RETURN a, b, r
      """
    Then the result should be:
      | a    | b               | r    |
      | null | (:B {prop: 46}) | null |
    And no side effects

  Scenario: Handling optional matches between nulls
    When executing query:
      """
      OPTIONAL MATCH (a:NotThere)
      OPTIONAL MATCH (b:NotThere)
      WITH a, b
      OPTIONAL MATCH (b)-[r:NOR_THIS]->(a)
      RETURN a, b, r
      """
    Then the result should be:
      | a    | b    | r    |
      | null | null | null |
    And no side effects

  Scenario: OPTIONAL MATCH and `collect()`
    And having executed:
      """
      CREATE (:DoesExist {property: 42})
      CREATE (:DoesExist {property: 43})
      CREATE (:DoesExist {property: 44})
      """
    When executing query:
      """
      OPTIONAL MATCH (f:DoesExist)
      OPTIONAL MATCH (n:DoesNotExist)
      RETURN collect(DISTINCT n.property) AS a, collect(DISTINCT f.property) AS b
      """
    Then the result should be:
      | a  | b            |
      | [] | [42, 43, 44] |
    And no side effects

  Scenario: Declaring a path with only one node in OPTIONAL MATCH after MATCH in which that node is already used
    When executing query:
      """
      MATCH (n1) OPTIONAL MATCH p=(n1) RETURN p;
      """
    Then the result should be:
      | p                 |
      | <(:Single)>       |
      | <(:A {prop: 42})> |
      | <(:B {prop: 46})> |
      | <(:C)>            |
    And no side effects
