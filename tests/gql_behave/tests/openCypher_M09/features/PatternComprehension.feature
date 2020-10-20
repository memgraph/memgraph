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

Feature: PatternComprehension

  Scenario: Pattern comprehension and ORDER BY
    Given an empty graph
    And having executed:
      """
      CREATE (a {time: 10}), (b {time: 20})
      CREATE (a)-[:T]->(b)
      """
    When executing query:
      """
      MATCH (liker)
      RETURN [p = (liker)--() | p] AS isNew
        ORDER BY liker.time
      """
    Then the result should be:
      | isNew                               |
      | [<({time: 10})-[:T]->({time: 20})>] |
      | [<({time: 20})<-[:T]-({time: 10})>] |
    And no side effects

  Scenario: Returning a pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      RETURN [p = (n)-->() | p] AS ps
      """
    Then the result should be:
      | ps                                     |
      | [<(:A)-[:T]->(:C)>, <(:A)-[:T]->(:B)>] |
      | []                                     |
      | []                                     |
    And no side effects

  Scenario: Returning a pattern comprehension with label predicate
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B), (c:C), (d:D)
      CREATE (a)-[:T]->(b),
             (a)-[:T]->(c),
             (a)-[:T]->(d)
      """
    When executing query:
      """
      MATCH (n:A)
      RETURN [p = (n)-->(:B) | p] AS x
      """
    Then the result should be:
      | x |
      | [<(:A)-[:T]->(:B)>] |
    And no side effects

  Scenario: Returning a pattern comprehension with bound nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:T]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      RETURN [p = (a)-[*]->(b) | p] AS paths
      """
    Then the result should be:
      | paths                |
      | [<(:A)-[:T]->(:B)>] |
    And no side effects

  Scenario: Using a pattern comprehension in a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)-->(b)
      WITH [p = (n)-->() | p] AS ps, count(b) AS c
      RETURN ps, c
      """
    Then the result should be:
      | ps                                      | c |
      | [<(:A)-[:T]->(:C)>, <(:A)-[:T]->(:B)>] | 2 |
    And no side effects

  Scenario: Using a variable-length pattern comprehension in a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T]->(:B)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      WITH [p = (a)-[*]->(b) | p] AS paths, count(a) AS c
      RETURN paths, c
      """
    Then the result should be:
      | paths                | c |
      | [<(:A)-[:T]->(:B)>] | 1 |
    And no side effects

  Scenario: Using pattern comprehension in RETURN
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (:A), (:A)
      CREATE (a)-[:HAS]->()
      """
    When executing query:
      """
      MATCH (n:A)
      RETURN [p = (n)-[:HAS]->() | p] AS ps
      """
    Then the result should be:
      | ps                   |
      | [<(:A)-[:HAS]->()>] |
      | []                  |
      | []                  |
    And no side effects

  Scenario: Aggregating on pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (:A), (:A)
      CREATE (a)-[:HAS]->()
      """
    When executing query:
      """
      MATCH (n:A)
      RETURN count([p = (n)-[:HAS]->() | p]) AS c
      """
    Then the result should be:
      | c |
      | 3 |
    And no side effects

  Scenario: Using pattern comprehension to test existence
    Given an empty graph
    And having executed:
      """
      CREATE (a:X {prop: 42}), (:X {prop: 43})
      CREATE (a)-[:T]->()
      """
    When executing query:
      """
      MATCH (n:X)
      RETURN n, size([(n)--() | 1]) > 0 AS b
      """
    Then the result should be:
      | n               | b     |
      | (:X {prop: 42}) | true  |
      | (:X {prop: 43}) | false |
    And no side effects

  Scenario: Pattern comprehension inside list comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (n1:X {n: 1}), (m1:Y), (i1:Y), (i2:Y)
      CREATE (n1)-[:T]->(m1),
             (m1)-[:T]->(i1),
             (m1)-[:T]->(i2)
      CREATE (n2:X {n: 2}), (m2), (i3:L), (i4:Y)
      CREATE (n2)-[:T]->(m2),
             (m2)-[:T]->(i3),
             (m2)-[:T]->(i4)
      """
    When executing query:
      """
      MATCH p = (n:X)-->(b)
      RETURN n, [x IN nodes(p) | size([(x)-->(:Y) | 1])] AS list
      """
    Then the result should be:
      | n           | list   |
      | (:X {n: 1}) | [1, 2] |
      | (:X {n: 2}) | [0, 1] |
    And no side effects

  Scenario: Get node degree via size of pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (x:X),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:T]->()
      """
    When executing query:
      """
      MATCH (a:X)
      RETURN size([(a)-->() | 1]) AS length
      """
    Then the result should be:
      | length |
      | 3      |
    And no side effects

  Scenario: Get node degree via size of pattern comprehension that specifies a relationship type
    Given an empty graph
    And having executed:
      """
      CREATE (x:X),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:OTHER]->()
      """
    When executing query:
      """
      MATCH (a:X)
      RETURN size([(a)-[:T]->() | 1]) AS length
      """
    Then the result should be:
      | length |
      | 3      |
    And no side effects

  Scenario: Get node degree via size of pattern comprehension that specifies multiple relationship types
    Given an empty graph
    And having executed:
      """
      CREATE (x:X),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:T]->(),
        (x)-[:OTHER]->()
      """
    When executing query:
      """
      MATCH (a:X)
      RETURN size([(a)-[:T|OTHER]->() | 1]) AS length
      """
    Then the result should be:
      | length |
      | 4      |
    And no side effects

  Scenario: Introducing new node variable in pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (a), (b {prop: 'val'})
      CREATE (a)-[:T]->(b)
      """
    When executing query:
      """
      MATCH (n)
      RETURN [(n)-[:T]->(b) | b.prop] AS list
      """
    Then the result should be:
      | list    |
      | ['val'] |
      | []      |
    And no side effects

  Scenario: Introducing new relationship variable in pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (a), (b)
      CREATE (a)-[:T {prop: 'val'}]->(b)
      """
    When executing query:
      """
      MATCH (n)
      RETURN [(n)-[r:T]->() | r.prop] AS list
      """
    Then the result should be:
      | list    |
      | ['val'] |
      | []      |
    And no side effects
