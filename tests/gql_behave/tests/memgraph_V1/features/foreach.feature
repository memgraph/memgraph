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

  Scenario: Foreach Foreach create
    Given an empty graph
    And having executed
      """
      FOREACH( i IN [1, 2, 3] | CREATE (n {age : i})) FOREACH( i in [4, 5, 6] | CREATE (n {age : i}))
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
      | 4 |
      | 5 |
      | 6 |
    And no side effects

  Scenario: Foreach shadowing
    Given an empty graph
    And having executed
      """
      FOREACH( i IN [1] | FOREACH( i in [2, 3, 4] | CREATE (n {age : i})))
      """
    When executing query:
      """
      MATCH (n) RETURN n.age
      """
    Then the result should be:
      | n.age |
      | 2 |
      | 3 |
      | 4 |
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
    FOREACH (n IN nodes(p) | SET n.marked = true)
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
    FOREACH (n IN nodes(p) | REMOVE n.marked)
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

  # Scenario: Foreach delete
  #   Given an empty graph
  #   And having executed
  #   """
  #   CREATE (n1 { marked: false })-[:RELATES]->(n2 { marked: false })
  #   """
  #   And having executed
  #   """
  #   MATCH p=(n1)-[*]->(n2)
  #   FOREACH (n IN nodes(p) | DETACH delete n)
  #   """
  #   When executing query:
  #     """
  #     MATCH (n)
  #     RETURN n;
  #     """
  #   Then the result should be:
  #     | |
  #   And no side effects

  Scenario: Foreach merge
    Given an empty graph
    And having executed
    """
    FOREACH (i IN [1, 2, 3] | MERGE (n { age : i }))
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

 Scenario: Foreach nested
   Given an empty graph
   And having executed
   """
   FOREACH (i IN [1, 2, 3] | FOREACH( j IN [1] | CREATE (k { prop : j })))
   """
   When executing query:
     """
     MATCH (n)
     RETURN n.prop;
     """
   Then the result should be:
     | n.prop |
     | 1 |
     | 1 |
     | 1 |

 Scenario: Foreach multiple update clauses
   Given an empty graph
   And having executed
   """
   CREATE (n1 { marked1: false, marked2: false })-[:RELATES]->(n2 { marked1: false, marked2: false })
   """
   And having executed
   """
   MATCH p=(n1)-[*]->(n2)
   FOREACH (n IN nodes(p) | SET n.marked1 = true SET n.marked2 = true)
   """
   When executing query:
   """
   MATCH (n)
   RETURN n
   """
   Then the result should be:
     | n |
     | ({marked1: true, marked2: true}) |
     | ({marked1: true, marked2: true}) |
   And no side effects

 Scenario: Foreach multiple nested update clauses
   Given an empty graph
   And having executed
   """
   CREATE (n1 { marked1: false, marked2: false })-[:RELATES]->(n2 { marked1: false, marked2: false })
   """
   And having executed
   """
   MATCH p=(n1)-[*]->(n2)
   FOREACH (n IN nodes(p) | FOREACH (j IN [1] | SET n.marked1 = true SET n.marked2 = true))
   """
   When executing query:
   """
   MATCH (n)
   RETURN n
   """
   Then the result should be:
     | n |
     | ({marked1: true, marked2: true}) |
     | ({marked1: true, marked2: true}) |
   And no side effects

 Scenario: Foreach match foreach return
   Given an empty graph
   And having executed
   """
   CREATE (n {prop: [[], [1,2]]});
   """
   When executing query:
   """
   MATCH (n) FOREACH (i IN n.prop | CREATE (:V { i: i})) RETURN n;
   """
   Then the result should be:
     | n |
     | ({prop: [[], [1, 2]]}) |
   And no side effects

 Scenario: Foreach on null value
   Given an empty graph
   And having executed
   """
   CREATE (n);
   """
   When executing query:
   """
   MATCH (n) FOREACH (i IN n.prop | CREATE (:V { i: i}));
   """
   Then the result should be:
     | |
   And no side effects

 Scenario: Foreach nested merge
   Given an empty graph
   And having executed
   """
   FOREACH(i in [1, 2, 3] | FOREACH(j in [2] | MERGE (n { age : i })));
   """
   When executing query:
   """
   MATCH (n)
   RETURN n
   """
   Then the result should be:
     | n |
     | ({age: 1}) |
     | ({age: 2}) |
     | ({age: 3}) |
   And no side effects
