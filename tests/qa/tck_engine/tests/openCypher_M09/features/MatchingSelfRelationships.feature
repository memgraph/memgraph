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

Feature: MatchingSelfRelationships

  Scenario: Undirected match in self-relationship graph
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH (a)-[r]-(b)
      RETURN a, r, b
      """
    Then the result should be:
      | a    | r       | b    |
      | (:A) | [:LOOP] | (:A) |
    And no side effects

  Scenario: Undirected match in self-relationship graph, count
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH ()--()
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And no side effects

  Scenario: Undirected match of self-relationship in self-relationship graph
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH (n)-[r]-(n)
      RETURN n, r
      """
    Then the result should be:
      | n    | r       |
      | (:A) | [:LOOP] |
    And no side effects

  Scenario: Undirected match of self-relationship in self-relationship graph, count
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH (n)--(n)
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And no side effects

  Scenario: Undirected match on simple relationship graph
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:LOOP]->(:B)
      """
    When executing query:
      """
      MATCH (a)-[r]-(b)
      RETURN a, r, b
      """
    Then the result should be:
      | a    | r       | b    |
      | (:A) | [:LOOP] | (:B) |
      | (:B) | [:LOOP] | (:A) |
    And no side effects

  Scenario: Undirected match on simple relationship graph, count
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:LOOP]->(:B)
      """
    When executing query:
      """
      MATCH ()--()
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 2        |
    And no side effects

  Scenario: Directed match on self-relationship graph
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH (a)-[r]->(b)
      RETURN a, r, b
      """
    Then the result should be:
      | a    | r       | b    |
      | (:A) | [:LOOP] | (:A) |
    And no side effects

  Scenario: Directed match on self-relationship graph, count
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH ()-->()
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And no side effects

  Scenario: Directed match of self-relationship on self-relationship graph
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH (n)-[r]->(n)
      RETURN n, r
      """
    Then the result should be:
      | n    | r       |
      | (:A) | [:LOOP] |
    And no side effects

  Scenario: Directed match of self-relationship on self-relationship graph, count
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH (n)-->(n)
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And no side effects

  Scenario: Counting undirected self-relationships in self-relationship graph
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH (n)-[r]-(n)
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And no side effects

  Scenario: Counting distinct undirected self-relationships in self-relationship graph
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a)
      """
    When executing query:
      """
      MATCH (n)-[r]-(n)
      RETURN count(DISTINCT r)
      """
    Then the result should be:
      | count(DISTINCT r) |
      | 1                 |
    And no side effects

  Scenario: Directed match of a simple relationship
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:LOOP]->(:B)
      """
    When executing query:
      """
      MATCH (a)-[r]->(b)
      RETURN a, r, b
      """
    Then the result should be:
      | a    | r       | b    |
      | (:A) | [:LOOP] | (:B) |
    And no side effects

  Scenario: Directed match of a simple relationship, count
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:LOOP]->(:B)
      """
    When executing query:
      """
      MATCH ()-->()
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And no side effects

  Scenario: Counting directed self-relationships
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)-[:LOOP]->(a),
             ()-[:T]->()
      """
    When executing query:
      """
      MATCH (n)-[r]->(n)
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And no side effects

  Scenario: Mixing directed and undirected pattern parts with self-relationship, simple
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T1]->(l:Looper),
             (l)-[:LOOP]->(l),
             (l)-[:T2]->(:B)
      """
    When executing query:
      """
      MATCH (x:A)-[r1]->(y)-[r2]-(z)
      RETURN x, r1, y, r2, z
      """
    Then the result should be:
      | x    | r1    | y         | r2      | z         |
      | (:A) | [:T1] | (:Looper) | [:LOOP] | (:Looper) |
      | (:A) | [:T1] | (:Looper) | [:T2]   | (:B)      |
    And no side effects

  Scenario: Mixing directed and undirected pattern parts with self-relationship, count
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T1]->(l:Looper),
             (l)-[:LOOP]->(l),
             (l)-[:T2]->(:B)
      """
    When executing query:
      """
      MATCH (:A)-->()--()
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 2        |
    And no side effects

  Scenario: Mixing directed and undirected pattern parts with self-relationship, undirected
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T1]->(l:Looper),
             (l)-[:LOOP]->(l),
             (l)-[:T2]->(:B)
      """
    When executing query:
      """
      MATCH (x)-[r1]-(y)-[r2]-(z)
      RETURN x, r1, y, r2, z
      """
    Then the result should be:
      | x         | r1      | y         | r2      | z         |
      | (:A)      | [:T1]   | (:Looper) | [:LOOP] | (:Looper) |
      | (:A)      | [:T1]   | (:Looper) | [:T2]   | (:B)      |
      | (:Looper) | [:LOOP] | (:Looper) | [:T1]   | (:A)      |
      | (:Looper) | [:LOOP] | (:Looper) | [:T2]   | (:B)      |
      | (:B)      | [:T2]   | (:Looper) | [:LOOP] | (:Looper) |
      | (:B)      | [:T2]   | (:Looper) | [:T1]   | (:A)      |
    And no side effects

  Scenario: Mixing directed and undirected pattern parts with self-relationship, undirected count
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T1]->(l:Looper),
             (l)-[:LOOP]->(l),
             (l)-[:T2]->(:B)
      """
    When executing query:
      """
      MATCH ()-[]-()-[]-()
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 6        |
    And no side effects
