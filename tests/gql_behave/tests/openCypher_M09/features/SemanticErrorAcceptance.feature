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

Feature: SemanticErrorAcceptance

  Background:
    Given any graph

  Scenario: Failing when returning an undefined variable
    When executing query:
      """
      MATCH ()
      RETURN foo
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Failing when comparing to an undefined variable
    When executing query:
      """
      MATCH (s)
      WHERE s.name = undefinedVariable
        AND s.age = 10
      RETURN s
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Failing when using IN on a string literal
    When executing query:
      """
      MATCH (n)
      WHERE n.id IN ''
      RETURN 1
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when using IN on an integer literal
    When executing query:
      """
      MATCH (n)
      WHERE n.id IN 1
      RETURN 1
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when using IN on a float literal
    When executing query:
      """
      MATCH (n)
      WHERE n.id IN 1.0
      RETURN 1
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when using IN on a boolean literal
    When executing query:
      """
      MATCH (n)
      WHERE n.id IN true
      RETURN 1
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when a node is used as a relationship
    When executing query:
      """
      MATCH (r)
      MATCH ()-[r]-()
      RETURN r
      """
    Then a SyntaxError should be raised at compile time: VariableTypeConflict

  Scenario: Failing when a relationship is used as a node
    When executing query:
      """
      MATCH ()-[r]-(r)
      RETURN r
      """
    Then a SyntaxError should be raised at compile time: VariableTypeConflict

  Scenario: Failing when using `type()` on a node
    When executing query:
      """
      MATCH (r)
      RETURN type(r)
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when using `length()` on a node
    When executing query:
      """
      MATCH (r)
      RETURN length(r)
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when re-using a relationship in the same pattern
    When executing query:
      """
      MATCH (a)-[r]->()-[r]->(a)
      RETURN r
      """
    Then a SyntaxError should be raised at compile time: RelationshipUniquenessViolation

  Scenario: Failing when using NOT on string literal
    When executing query:
      """
      RETURN NOT 'foo'
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when using variable length relationship in CREATE
    When executing query:
      """
      CREATE ()-[:FOO*2]->()
      """
    Then a SyntaxError should be raised at compile time: CreatingVarLength

  Scenario: Failing when using variable length relationship in MERGE
    When executing query:
      """
      MERGE (a)
      MERGE (b)
      MERGE (a)-[:FOO*2]->(b)
      """
    Then a SyntaxError should be raised at compile time: CreatingVarLength

  Scenario: Failing when using parameter as node predicate in MATCH
    When executing query:
      """
      MATCH (n $param)
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: InvalidParameterUse

  Scenario: Failing when using parameter as relationship predicate in MATCH
    When executing query:
      """
      MATCH ()-[r:FOO $param]->()
      RETURN r
      """
    Then a SyntaxError should be raised at compile time: InvalidParameterUse

  Scenario: Failing when using parameter as node predicate in MERGE
    When executing query:
      """
      MERGE (n $param)
      RETURN n
      """
    Then a SyntaxError should be raised at compile time: InvalidParameterUse

  Scenario: Failing when using parameter as relationship predicate in MERGE
    When executing query:
      """
      MERGE (a)
      MERGE (b)
      MERGE (a)-[r:FOO $param]->(b)
      RETURN r
      """
    Then a SyntaxError should be raised at compile time: InvalidParameterUse

  Scenario: Failing when deleting an integer expression
    When executing query:
      """
      MATCH ()
      DELETE 1 + 1
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when using CREATE on a node that is already bound
    When executing query:
      """
      MATCH (a)
      CREATE (a)
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: Failing when using MERGE on a node that is already bound
    When executing query:
      """
      MATCH (a)
      CREATE (a)
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: Failing when using CREATE on a relationship that is already bound
    When executing query:
      """
      MATCH ()-[r]->()
      CREATE ()-[r]->()
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: Failing when using MERGE on a relationship that is already bound
    When executing query:
      """
      MATCH (a)-[r]->(b)
      MERGE (a)-[r]->(b)
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: Failing when using undefined variable in ON CREATE
    When executing query:
      """
      MERGE (n)
        ON CREATE SET x.foo = 1
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Failing when using undefined variable in ON MATCH
    When executing query:
      """
      MERGE (n)
        ON MATCH SET x.foo = 1
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Failing when using MATCH after OPTIONAL MATCH
    When executing query:
      """
      OPTIONAL MATCH ()-->()
      MATCH ()-->(d)
      RETURN d
      """
    Then a SyntaxError should be raised at compile time: InvalidClauseComposition

  Scenario: Failing when float value is too large
    When executing query:
      """
      RETURN 1.34E999
      """
    Then a SyntaxError should be raised at compile time: FloatingPointOverflow

  Scenario: Handling property access on the Any type
    When executing query:
      """
      WITH [{prop: 0}, 1] AS list
      RETURN (list[0]).prop
      """
    Then the result should be:
      | (list[0]).prop |
      | 0              |
    And no side effects

  Scenario: Failing when performing property access on a non-map 1
    When executing query:
      """
      WITH [{prop: 0}, 1] AS list
      RETURN (list[1]).prop
      """
    Then a TypeError should be raised at runtime: PropertyAccessOnNonMap

  Scenario: Failing when performing property access on a non-map 2
    When executing query:
      """
      CREATE (n {prop: 'foo'})
      WITH n.prop AS n2
      RETURN n2.prop
      """
    Then a TypeError should be raised at runtime: PropertyAccessOnNonMap

  Scenario: Failing when checking existence of a non-property and non-pattern
    When executing query:
      """
      MATCH (n)
      RETURN exists(n.prop + 1)
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentExpression

  Scenario: Bad arguments for `range()`
    When executing query:
      """
      RETURN range(2, 8, 0)
      """
    Then a ArgumentError should be raised at runtime: NumberOutOfRange

  Scenario: Fail for invalid Unicode hyphen in subtraction
    When executing query:
      """
      RETURN 42 — 41
      """
    Then a SyntaxError should be raised at compile time: InvalidUnicodeCharacter

  Scenario: Failing for `size()` on paths
    When executing query:
      """
      MATCH p = (a)-[*]->(b)
      RETURN size(p)
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when using aggregation in list comprehension
    When executing query:
      """
      MATCH (n)
      RETURN [x IN [1, 2, 3, 4, 5] | count(*)]
      """
    Then a SyntaxError should be raised at compile time: InvalidAggregation

  Scenario: Failing when using non-constants in SKIP
    When executing query:
      """
      MATCH (n)
      RETURN n
        SKIP n.count
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: Failing when using negative value in SKIP
    When executing query:
      """
      MATCH (n)
      RETURN n
        SKIP -1
      """
    Then a SyntaxError should be raised at compile time: NegativeIntegerArgument

  Scenario: Failing when using non-constants in LIMIT
    When executing query:
      """
      MATCH (n)
      RETURN n
        LIMIT n.count
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: Failing when using negative value in LIMIT
    When executing query:
      """
      MATCH (n)
      RETURN n
        LIMIT -1
      """
    Then a SyntaxError should be raised at compile time: NegativeIntegerArgument

  Scenario: Failing when using floating point in LIMIT
    When executing query:
      """
      MATCH (n)
      RETURN n
        LIMIT 1.7
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: Failing when creating relationship without type
    When executing query:
      """
      CREATE ()-->()
      """
    Then a SyntaxError should be raised at compile time: NoSingleRelationshipType

  Scenario: Failing when merging relationship without type
    When executing query:
      """
      CREATE (a), (b)
      MERGE (a)-->(b)
      """
    Then a SyntaxError should be raised at compile time: NoSingleRelationshipType

  Scenario: Failing when merging relationship without type, no colon
    When executing query:
      """
      MATCH (a), (b)
      MERGE (a)-[NO_COLON]->(b)
      """
    Then a SyntaxError should be raised at compile time: NoSingleRelationshipType

  Scenario: Failing when creating relationship with more than one type
    When executing query:
      """
      CREATE ()-[:A|:B]->()
      """
    Then a SyntaxError should be raised at compile time: NoSingleRelationshipType

  Scenario: Failing when merging relationship with more than one type
    When executing query:
      """
      CREATE (a), (b)
      MERGE (a)-[:A|:B]->(b)
      """
    Then a SyntaxError should be raised at compile time: NoSingleRelationshipType
