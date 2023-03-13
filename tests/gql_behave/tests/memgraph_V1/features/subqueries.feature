# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

Feature: Subqueries
  Behaviour tests for memgraph CALL clause which contains a subquery

  Scenario: Subquery without bounded symbols
    Given an empty graph
    And having executed
      """
      CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
      """
    When executing query:
      """
      MATCH (n:Label1)
      CALL {
        MATCH (n:Label1)-[:TYPE]->(m:Label2)
        RETURN m;
      }
      RETURN m.prop;
      """
    Then the result should be:
      | m.prop |
      | 2      |

  Scenario: Subquery without bounded symbols and without match
    Given an empty graph
    And having executed
      """
      CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
      """
    When executing query:
      """
      CALL {
        MATCH (n:Label1)-[:TYPE]->(m:Label2)
        RETURN m;
      }
      RETURN m.prop;
      """
    Then the result should be:
      | m.prop |
      | 2      |
