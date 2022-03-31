# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

Feature: Foreach
  Behaviour tests for memgraph FOREACH clause

  Scenario: Foreach create
    Given an empty graph
    And having executed
      """
      FOREACH( i IN [1, 2, 3] | CREATE (n {age : i}))
      """
    When executing query:
      """
      MATCH (n) RETURN n.age
      """
    Then the result should be:
      | n.age |
      | 1 |
      | 2 |
      | 3 |
    And no side effects

  Scenario: Foreach set
    Given an empty graph
    And having executed
    """
    CREATE (n1 { marked: false })-[:RELATES]->(n2 { marked: false })
    """
    And having executed 
    """
    MATCH p=(n1)-[*]->(n2)
    FOREACH (n IN nodes(p) | set n.marked = true)
    """
    When executing query:
      """
      MATCH (n)
      RETURN n.marked
      """
    Then the result should be:
      | n.marked |
      | true |
      | true |
    And no side effects

  Scenario: Foreach remove
    Given an empty graph
    And having executed
    """
    CREATE (n1 { marked: false })-[:RELATES]->(n2 { marked: false })
    """
    And having executed 
    """
    MATCH p=(n1)-[*]->(n2)
    FOREACH (n IN nodes(p) | remove n.marked)
    """
    When executing query:
      """
      MATCH (n)
      RETURN n;
      """
    Then the result should be:
      | n |
      | () |
      | () |
    And no side effects

  Scenario: Foreach delete
    Given an empty graph
    And having executed
    """
    CREATE (n1 { marked: false })-[:RELATES]->(n2 { marked: false })
    """
    And having executed 
    """
    MATCH p=(n1)-[*]->(n2)
    FOREACH (n IN nodes(p) | detach delete n)
    """
    When executing query:
      """
      MATCH (n)
      RETURN n;
      """
    Then the result should be:
      | |
    And no side effects

  Scenario: Foreach merge
    Given an empty graph
    And having executed 
    """
    FOREACH (i IN [1, 2, 3] | MERGE (n {age : i}))
    """
    When executing query:
      """
      MATCH (n)
      RETURN n.age;
      """
    Then the result should be:
      | n.age |
      | 1 |
      | 2 |
      | 3 |
    And no side effects

#  Scenario: Foreach nested
#    Given an empty graph
#    And having executed 
#    """
#    FOREACH (i IN [1, 2, 3] | FOREACH( j IN [1] | CREATE (k { prop : j})))
#    """
#    When executing query:
#      """
#      MATCH (n)
#      RETURN n.prop;
#      """
#    Then the result should be:
#      | n.prop |
#      | 1 |
#      | 1 |
#      | 1 |

#  Scenario: Foreach multiple update clauses   #    And no side effects
#    Given an empty graph
#    And no side effects
