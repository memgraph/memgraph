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

Feature: MatchAcceptance

  Scenario: Path query should return results in written order
    Given an empty graph
    And having executed:
      """
      CREATE (:Label1)<-[:TYPE]-(:Label2)
      """
    When executing query:
      """
      MATCH p = (a:Label1)<--(:Label2)
      RETURN p
      """
    Then the result should be:
      | p                              |
      | <(:Label1)<-[:TYPE]-(:Label2)> |
    And no side effects

  Scenario: Longer path query should return results in written order
    Given an empty graph
    And having executed:
      """
      CREATE (:Label1)<-[:T1]-(:Label2)-[:T2]->(:Label3)
      """
    When executing query:
      """
      MATCH p = (a:Label1)<--(:Label2)--()
      RETURN p
      """
    Then the result should be:
      | p                                             |
      | <(:Label1)<-[:T1]-(:Label2)-[:T2]->(:Label3)> |
    And no side effects

  Scenario: Use multiple MATCH clauses to do a Cartesian product
    Given an empty graph
    And having executed:
      """
      CREATE ({value: 1}),
        ({value: 2}),
        ({value: 3})
      """
    When executing query:
      """
      MATCH (n), (m)
      RETURN n.value AS n, m.value AS m
      """
    Then the result should be:
      | n | m |
      | 1 | 1 |
      | 1 | 2 |
      | 1 | 3 |
      | 2 | 1 |
      | 2 | 2 |
      | 2 | 3 |
      | 3 | 3 |
      | 3 | 1 |
      | 3 | 2 |
    And no side effects

  Scenario: Use params in pattern matching predicates
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T {foo: 'bar'}]->(:B {name: 'me'})
      """
    And parameters are:
      | param | 'bar' |
    When executing query:
      """
      MATCH (a)-[r]->(b)
      WHERE r.foo = $param
      RETURN b
      """
    Then the result should be:
      | b                 |
      | (:B {name: 'me'}) |
    And no side effects

  Scenario: Filter out based on node prop name
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'Someone'})<-[:X]-()-[:X]->({name: 'Andres'})
      """
    When executing query:
      """
      MATCH ()-[rel:X]-(a)
      WHERE a.name = 'Andres'
      RETURN a
      """
    Then the result should be:
      | a                  |
      | ({name: 'Andres'}) |
    And no side effects

  Scenario: Honour the column name for RETURN items
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'Someone'})
      """
    When executing query:
      """
      MATCH (a)
      WITH a.name AS a
      RETURN a
      """
    Then the result should be:
      | a         |
      | 'Someone' |
    And no side effects

  Scenario: Filter based on rel prop name
    Given an empty graph
    And having executed:
      """
      CREATE (:A)<-[:KNOWS {name: 'monkey'}]-()-[:KNOWS {name: 'woot'}]->(:B)
      """
    When executing query:
      """
      MATCH (node)-[r:KNOWS]->(a)
      WHERE r.name = 'monkey'
      RETURN a
      """
    Then the result should be:
      | a    |
      | (:A) |
    And no side effects

  Scenario: Cope with shadowed variables
    Given an empty graph
    And having executed:
      """
      CREATE ({value: 1, name: 'King Kong'}),
        ({value: 2, name: 'Ann Darrow'})
      """
    When executing query:
      """
      MATCH (n)
      WITH n.name AS n
      RETURN n
      """
    Then the result should be:
      | n            |
      | 'Ann Darrow' |
      | 'King Kong'  |
    And no side effects

  Scenario: Get neighbours
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {value: 1})-[:KNOWS]->(b:B {value: 2})
      """
    When executing query:
      """
      MATCH (n1)-[rel:KNOWS]->(n2)
      RETURN n1, n2
      """
    Then the result should be:
      | n1              | n2              |
      | (:A {value: 1}) | (:B {value: 2}) |
    And no side effects

  Scenario: Get two related nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {value: 1}),
        (a)-[:KNOWS]->(b:B {value: 2}),
        (a)-[:KNOWS]->(c:C {value: 3})
      """
    When executing query:
      """
      MATCH ()-[rel:KNOWS]->(x)
      RETURN x
      """
    Then the result should be:
      | x               |
      | (:B {value: 2}) |
      | (:C {value: 3}) |
    And no side effects

  Scenario: Get related to related to
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {value: 1})-[:KNOWS]->(b:B {value: 2})-[:FRIEND]->(c:C {value: 3})
      """
    When executing query:
      """
      MATCH (n)-->(a)-->(b)
      RETURN b
      """
    Then the result should be:
      | b               |
      | (:C {value: 3}) |
    And no side effects

  Scenario: Handle comparison between node properties
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {animal: 'monkey'}),
        (b:B {animal: 'cow'}),
        (c:C {animal: 'monkey'}),
        (d:D {animal: 'cow'}),
        (a)-[:KNOWS]->(b),
        (a)-[:KNOWS]->(c),
        (d)-[:KNOWS]->(b),
        (d)-[:KNOWS]->(c)
      """
    When executing query:
      """
      MATCH (n)-[rel]->(x)
      WHERE n.animal = x.animal
      RETURN n, x
      """
    Then the result should be:
      | n                       | x                       |
      | (:A {animal: 'monkey'}) | (:C {animal: 'monkey'}) |
      | (:D {animal: 'cow'})    | (:B {animal: 'cow'})    |
    And no side effects

  Scenario: Return two subgraphs with bound undirected relationship
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {value: 1})-[:REL {name: 'r'}]->(b:B {value: 2})
      """
    When executing query:
      """
      MATCH (a)-[r {name: 'r'}]-(b)
      RETURN a, b
      """
    Then the result should be:
      | a               | b               |
      | (:B {value: 2}) | (:A {value: 1}) |
      | (:A {value: 1}) | (:B {value: 2}) |
    And no side effects

  Scenario: Return two subgraphs with bound undirected relationship and optional relationship
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {value: 1})-[:REL {name: 'r1'}]->(b:B {value: 2})-[:REL {name: 'r2'}]->(c:C {value: 3})
      """
    When executing query:
      """
      MATCH (a)-[r {name: 'r1'}]-(b)
      OPTIONAL MATCH (b)-[r2]-(c)
      WHERE r <> r2
      RETURN a, b, c
      """
    Then the result should be:
      | a               | b               | c               |
      | (:A {value: 1}) | (:B {value: 2}) | (:C {value: 3}) |
      | (:B {value: 2}) | (:A {value: 1}) | null            |
    And no side effects

  Scenario: Rel type function works as expected
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {name: 'A'}),
        (b:B {name: 'B'}),
        (c:C {name: 'C'}),
        (a)-[:KNOWS]->(b),
        (a)-[:HATES]->(c)
      """
    When executing query:
      """
      MATCH (n {name: 'A'})-[r]->(x)
      WHERE type(r) = 'KNOWS'
      RETURN x
      """
    Then the result should be:
      | x                |
      | (:B {name: 'B'}) |
    And no side effects

  Scenario: Walk alternative relationships
    Given an empty graph
    And having executed:
      """
      CREATE (a {name: 'A'}),
        (b {name: 'B'}),
        (c {name: 'C'}),
        (a)-[:KNOWS]->(b),
        (a)-[:HATES]->(c),
        (a)-[:WONDERS]->(c)
      """
    When executing query:
      """
      MATCH (n)-[r]->(x)
      WHERE type(r) = 'KNOWS' OR type(r) = 'HATES'
      RETURN r
      """
    Then the result should be:
      | r        |
      | [:KNOWS] |
      | [:HATES] |
    And no side effects

  Scenario: Handle OR in the WHERE clause
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {p1: 12}),
        (b:B {p2: 13}),
        (c:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE n.p1 = 12 OR n.p2 = 13
      RETURN n
      """
    Then the result should be:
      | n             |
      | (:A {p1: 12}) |
      | (:B {p2: 13}) |
    And no side effects

  Scenario: Return a simple path
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {name: 'A'})-[:KNOWS]->(b:B {name: 'B'})
      """
    When executing query:
      """
      MATCH p = (a {name: 'A'})-->(b)
      RETURN p
      """
    Then the result should be:
      | p                                             |
      | <(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})> |
    And no side effects

  Scenario: Return a three node path
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {name: 'A'})-[:KNOWS]->(b:B {name: 'B'})-[:KNOWS]->(c:C {name: 'C'})
      """
    When executing query:
      """
      MATCH p = (a {name: 'A'})-[rel1]->(b)-[rel2]->(c)
      RETURN p
      """
    Then the result should be:
      | p                                                                        |
      | <(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})-[:KNOWS]->(:C {name: 'C'})> |
    And no side effects

  Scenario: Do not return anything because path length does not match
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {name: 'A'})-[:KNOWS]->(b:B {name: 'B'})
      """
    When executing query:
      """
      MATCH p = (n)-->(x)
      WHERE length(p) = 10
      RETURN x
      """
    Then the result should be:
      | x |
    And no side effects

  Scenario: Pass the path length test
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {name: 'A'})-[:KNOWS]->(b:B {name: 'B'})
      """
    When executing query:
      """
      MATCH p = (n)-->(x)
      WHERE length(p) = 1
      RETURN x
      """
    Then the result should be:
      | x                |
      | (:B {name: 'B'}) |
    And no side effects

  Scenario: Return relationships by fetching them from the path - starting from the end
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(e:End)
      """
    When executing query:
      """
      MATCH p = (a)-[:REL*2..2]->(b:End)
      RETURN relationships(p)
      """
    Then the result should be:
      | relationships(p)                       |
      | [[:REL {value: 1}], [:REL {value: 2}]] |
    And no side effects

  Scenario: Return relationships by fetching them from the path
    Given an empty graph
    And having executed:
      """
      CREATE (s:Start)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:C)
      """
    When executing query:
      """
      MATCH p = (a:Start)-[:REL*2..2]->(b)
      RETURN relationships(p)
      """
    Then the result should be:
      | relationships(p)                       |
      | [[:REL {value: 1}], [:REL {value: 2}]] |
    And no side effects

  Scenario: Return relationships by collecting them as a list - directed, one way
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(e:End)
      """
    When executing query:
      """
      MATCH (a)-[r:REL*2..2]->(b:End)
      RETURN r
      """
    Then the result should be:
      | r                                      |
      | [[:REL {value: 1}], [:REL {value: 2}]] |
    And no side effects

  Scenario: Return relationships by collecting them as a list - undirected, starting from two extremes
    Given an empty graph
    And having executed:
      """
      CREATE (a:End)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:End)
      """
    When executing query:
      """
      MATCH (a)-[r:REL*2..2]-(b:End)
      RETURN r
      """
    Then the result should be:
      | r                                    |
      | [[:REL {value:1}], [:REL {value:2}]] |
      | [[:REL {value:2}], [:REL {value:1}]] |
    And no side effects

  Scenario: Return relationships by collecting them as a list - undirected, starting from one extreme
    Given an empty graph
    And having executed:
      """
      CREATE (s:Start)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:C)
      """
    When executing query:
      """
      MATCH (a:Start)-[r:REL*2..2]-(b)
      RETURN r
      """
    Then the result should be:
      | r                                      |
      | [[:REL {value: 1}], [:REL {value: 2}]] |
    And no side effects

  Scenario: Return a var length path
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {name: 'A'})-[:KNOWS {value: 1}]->(b:B {name: 'B'})-[:KNOWS {value: 2}]->(c:C {name: 'C'})
      """
    When executing query:
      """
      MATCH p = (n {name: 'A'})-[:KNOWS*1..2]->(x)
      RETURN p
      """
    Then the result should be:
      | p                                                                                              |
      | <(:A {name: 'A'})-[:KNOWS {value: 1}]->(:B {name: 'B'})>                                       |
      | <(:A {name: 'A'})-[:KNOWS {value: 1}]->(:B {name: 'B'})-[:KNOWS {value: 2}]->(:C {name: 'C'})> |
    And no side effects

  Scenario: Return a var length path of length zero
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:REL]->(b:B)
      """
    When executing query:
      """
      MATCH p = (a)-[*0..1]->(b)
      RETURN a, b, length(p) AS l
      """
    Then the result should be:
      | a    | b    | l |
      | (:A) | (:A) | 0 |
      | (:B) | (:B) | 0 |
      | (:A) | (:B) | 1 |
    And no side effects

  Scenario: Return a named var length path of length zero
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {name: 'A'})-[:KNOWS]->(b:B {name: 'B'})-[:FRIEND]->(c:C {name: 'C'})
      """
    When executing query:
      """
      MATCH p = (a {name: 'A'})-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c)
      RETURN p
      """
    Then the result should be:
      | p                                                                         |
      | <(:A {name: 'A'})>                                                        |
      | <(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})>                             |
      | <(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})-[:FRIEND]->(:C {name: 'C'})> |
    And no side effects

  Scenario: Accept skip zero
    Given any graph
    When executing query:
      """
      MATCH (n)
      WHERE 1 = 0
      RETURN n SKIP 0
      """
    Then the result should be:
      | n |
    And no side effects
