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

  Scenario: Subquery returning 2 values
    Given an empty graph
    And having executed
      """
      CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
      """
    When executing query:
      """
			MATCH (n:Label1)
      CALL {
        MATCH (m:Label1)-[:TYPE]->(o:Label2)
        RETURN m, o;
      }
      RETURN m.prop, o.prop;
      """
    Then the result should be:
      | m.prop | o.prop |
      | 1      | 2      |

  Scenario: Subquery returning nothing because match did not find any results
    Given an empty graph
    And having executed
      """
      CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
      """
    When executing query:
      """
			MATCH (n:Label3)
      CALL {
        MATCH (m:Label1)-[:TYPE]->(:Label2)
        RETURN m;
      }
      RETURN m.prop;
      """
    Then the result should be empty

  Scenario: Subquery returning a multiple of results since we join elements from basic query and the subquery
    Given an empty graph
    And having executed
      """
      CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
      """
    When executing query:
      """
			MATCH (n:Label1)
      CALL {
        MATCH (m)
        RETURN m;
      }
      RETURN n.prop, m.prop;
      """
    Then the result should be:
      | n.prop | m.prop |
      | 1      | 1      |
      | 1      | 2      |

  Scenario: Subquery returning a cartesian product
    Given an empty graph
    And having executed
      """
      CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
      """
    When executing query:
      """
			MATCH (n)
      CALL {
        MATCH (m)
        RETURN m;
      }
      RETURN n.prop, m.prop;
      """
    Then the result should be:
      | n.prop | m.prop |
      | 1      | 1      |
      | 1      | 2      |
      | 2      | 1      |
      | 2      | 2      |

  Scenario: Subquery with bounded symbols
    Given an empty graph
    And having executed
      """
      CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
      """
    When executing query:
      """
			MATCH (n:Label1)
      CALL {
				WITH n
        MATCH (n)-[:TYPE]->(m:Label2)
        RETURN m;
      }
      RETURN m.prop;
      """
    Then the result should be:
      | m.prop |
      | 2      |

  Scenario: Subquery with invalid bounded symbols
    Given an empty graph
    And having executed
      """
      CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
      """
    When executing query:
      """
			MATCH (n:Label1)
      CALL {
				WITH o
        MATCH (o)-[:TYPE]->(m:Label2)
        RETURN m;
      }
      RETURN m.prop;
      """
    Then an error should be raised

  Scenario: Subquery with union
    Given an empty graph
    And having executed
      """
      CREATE (:Person {figure: "grandpa"})<-[:CHILD_OF]-(:Person {figure: "dad"})-[:FATHER_OF]->(:Person {figure: "child"})
      """
    When executing query:
      """
			MATCH (p:Person)
			CALL {
					WITH p
					OPTIONAL MATCH (p)-[:CHILD_OF]->(other:Person)
					RETURN other
				UNION
					WITH p
					OPTIONAL MATCH (p)-[:PARENT_OF]->(other:Parent)
					RETURN other
			}
			RETURN DISTINCT p.figure, count(other) as cnt
      """
    Then the result should be:
      | p.figure | cnt |
      | "dad"    | 2   |
