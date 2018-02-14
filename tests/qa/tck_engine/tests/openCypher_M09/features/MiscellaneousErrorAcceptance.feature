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

Feature: MiscellaneousErrorAcceptance

  Background:
    Given any graph

  Scenario: Failing on incorrect unicode literal
    When executing query:
      """
      RETURN '\uH'
      """
    Then a SyntaxError should be raised at compile time: InvalidUnicodeLiteral

  Scenario: Failing on merging relationship with null property
    When executing query:
      """
      CREATE (a), (b)
      MERGE (a)-[r:X {p: null}]->(b)
      """
    Then a SemanticError should be raised at compile time: MergeReadOwnWrites

  Scenario: Failing on merging node with null property
    When executing query:
      """
      MERGE ({p: null})
      """
    Then a SemanticError should be raised at compile time: MergeReadOwnWrites

  Scenario: Failing on aggregation in WHERE
    When executing query:
      """
      MATCH (a)
      WHERE count(a) > 10
      RETURN a
      """
    Then a SyntaxError should be raised at compile time: InvalidAggregation

  Scenario: Failing on aggregation in ORDER BY after RETURN
    When executing query:
      """
      MATCH (n)
      RETURN n.prop1
        ORDER BY max(n.prop2)
      """
    Then a SyntaxError should be raised at compile time: InvalidAggregation

  Scenario: Failing on aggregation in ORDER BY after WITH
    When executing query:
      """
      MATCH (n)
      WITH n.prop1 AS foo
        ORDER BY max(n.prop2)
      RETURN foo AS foo
      """
    Then a SyntaxError should be raised at compile time: InvalidAggregation

  Scenario: Failing when not aliasing expressions in WITH
    When executing query:
      """
      MATCH (a)
      WITH a, count(*)
      RETURN a
      """
    Then a SyntaxError should be raised at compile time: NoExpressionAlias

  Scenario: Failing when using undefined variable in pattern
    When executing query:
      """
      MATCH (a)
      CREATE (a)-[:KNOWS]->(b {name: missing})
      RETURN b
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Failing when using undefined variable in SET
    When executing query:
      """
      MATCH (a)
      SET a.name = missing
      RETURN a
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Failing when using undefined variable in DELETE
    When executing query:
      """
      MATCH (a)
      DELETE x
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Failing when using a variable that is already bound in CREATE
    When executing query:
      """
      MATCH (a)
      CREATE (a {name: 'foo'})
      RETURN a
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: Failing when using a path variable that is already bound
    When executing query:
      """
      MATCH p = (a)
      WITH p, a
      MATCH p = (a)-->(b)
      RETURN a
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: Failing when using a list as a node
    When executing query:
      """
      MATCH (n)
      WITH [n] AS users
      MATCH (users)-->(messages)
      RETURN messages
      """
    Then a SyntaxError should be raised at compile time: VariableTypeConflict

  Scenario: Failing when using a variable length relationship as a single relationship
    When executing query:
      """
      MATCH (n)
      MATCH (n)-[r*]->()
      WHERE r.foo = 'apa'
      RETURN r
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when UNION has different columns
    When executing query:
      """
      RETURN 1 AS a
      UNION
      RETURN 2 AS b
      """
    Then a SyntaxError should be raised at compile time: DifferentColumnsInUnion

  Scenario: Failing when mixing UNION and UNION ALL
    When executing query:
      """
      RETURN 1 AS a
      UNION
      RETURN 2 AS a
      UNION ALL
      RETURN 3 AS a
      """
    Then a SyntaxError should be raised at compile time: InvalidClauseComposition

  Scenario: Failing when creating without direction
    When executing query:
      """
      CREATE (a)-[:FOO]-(b)
      """
    Then a SyntaxError should be raised at compile time: RequiresDirectedRelationship

  Scenario: Failing when creating with two directions
    When executing query:
      """
      CREATE (a)<-[:FOO]->(b)
      """
    Then a SyntaxError should be raised at compile time: RequiresDirectedRelationship

  Scenario: Failing when deleting a label
    When executing query:
      """
      MATCH (n)
      DELETE n:Person
      """
    Then a SyntaxError should be raised at compile time: InvalidDelete

  Scenario: Failing when setting a list of maps as a property
    When executing query:
      """
      CREATE (a)
      SET a.foo = [{x: 1}]
      """
    Then a TypeError should be raised at compile time: InvalidPropertyType

  Scenario: Failing when multiple columns have the same name
    When executing query:
      """
      RETURN 1 AS a, 2 AS a
      """
    Then a SyntaxError should be raised at compile time: ColumnNameConflict

  Scenario: Failing when using RETURN * without variables in scope
    When executing query:
      """
      MATCH ()
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: NoVariablesInScope
