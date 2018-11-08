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

Feature: OptionalMatch

  Scenario: Satisfies the open world assumption, relationships between same nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:Player), (b:Team)
      CREATE (a)-[:PLAYS_FOR]->(b),
             (a)-[:SUPPORTS]->(b)
      """
    When executing query:
      """
      MATCH (p:Player)-[:PLAYS_FOR]->(team:Team)
      OPTIONAL MATCH (p)-[s:SUPPORTS]->(team)
      RETURN count(*) AS matches, s IS NULL AS optMatch
      """
    Then the result should be:
      | matches | optMatch |
      | 1       | false    |
    And no side effects

  Scenario: Satisfies the open world assumption, single relationship
    Given an empty graph
    And having executed:
      """
      CREATE (a:Player), (b:Team)
      CREATE (a)-[:PLAYS_FOR]->(b)
      """
    When executing query:
      """
      MATCH (p:Player)-[:PLAYS_FOR]->(team:Team)
      OPTIONAL MATCH (p)-[s:SUPPORTS]->(team)
      RETURN count(*) AS matches, s IS NULL AS optMatch
      """
    Then the result should be:
      | matches | optMatch |
      | 1       | true     |
    And no side effects

  Scenario: Satisfies the open world assumption, relationships between different nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:Player), (b:Team), (c:Team)
      CREATE (a)-[:PLAYS_FOR]->(b),
             (a)-[:SUPPORTS]->(c)
      """
    When executing query:
      """
      MATCH (p:Player)-[:PLAYS_FOR]->(team:Team)
      OPTIONAL MATCH (p)-[s:SUPPORTS]->(team)
      RETURN count(*) AS matches, s IS NULL AS optMatch
      """
    Then the result should be:
      | matches | optMatch |
      | 1       | true     |
    And no side effects
