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

Feature: ReturnAcceptance2

  Scenario: Fail when returning properties of deleted nodes
    Given an empty graph
    And having executed:
      """
      CREATE ({p: 0})
      """
    When executing query:
      """
      MATCH (n)
      DELETE n
      RETURN n.p
      """
    Then a EntityNotFound should be raised at runtime: DeletedEntityAccess

  Scenario: Fail when returning labels of deleted nodes
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n)
      DELETE n
      RETURN labels(n)
      """
    Then a EntityNotFound should be raised at runtime: DeletedEntityAccess

  Scenario: Fail when returning properties of deleted relationships
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T {p: 0}]->()
      """
    When executing query:
      """
      MATCH ()-[r]->()
      DELETE r
      RETURN r.p
      """
    Then a EntityNotFound should be raised at runtime: DeletedEntityAccess

  Scenario: Do not fail when returning type of deleted relationships
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH ()-[r]->()
      DELETE r
      RETURN type(r)
      """
    Then the result should be:
      | type(r) |
      | 'T'     |
    And the side effects should be:
      | -relationships | 1 |

  Scenario: Accept valid Unicode literal
    Given any graph
    When executing query:
      """
      RETURN '\u01FF' AS a
      """
    Then the result should be:
      | a   |
      | 'ǿ' |
    And no side effects

  Scenario: LIMIT 0 should return an empty result
    Given an empty graph
    And having executed:
      """
      CREATE (), (), ()
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
        LIMIT 0
      """
    Then the result should be:
      | n |
    And no side effects

  Scenario: Fail when sorting on variable removed by DISTINCT
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A', age: 13}), ({name: 'B', age: 12}), ({name: 'C', age: 11})
      """
    When executing query:
      """
      MATCH (a)
      RETURN DISTINCT a.name
        ORDER BY a.age
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Ordering with aggregation
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'nisse'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n.name, count(*) AS foo
        ORDER BY n.name
      """
    Then the result should be:
      | n.name  | foo |
      | 'nisse' | 1   |
    And no side effects

  Scenario: DISTINCT on nullable values
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'Florescu'}), (), ()
      """
    When executing query:
      """
      MATCH (n)
      RETURN DISTINCT n.name
      """
    Then the result should be:
      | n.name     |
      | 'Florescu' |
      | null       |
    And no side effects

  Scenario: Return all variables
    Given an empty graph
    And having executed:
      """
      CREATE (:Start)-[:T]->()
      """
    When executing query:
      """
      MATCH p = (a:Start)-->(b)
      RETURN *
      """
    Then the result should be:
      | a        | b  | p                   |
      | (:Start) | () | <(:Start)-[:T]->()> |
    And no side effects

  Scenario: Setting and returning the size of a list property
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      SET n.x = [1, 2, 3]
      RETURN size(n.x)
      """
    Then the result should be:
      | size(n.x) |
      | 3         |
    And the side effects should be:
      | +properties | 1 |

  Scenario: `sqrt()` returning float values
    Given any graph
    When executing query:
      """
      RETURN sqrt(12.96)
      """
    Then the result should be:
      | sqrt(12.96) |
      | 3.6         |
    And no side effects

  Scenario: Arithmetic expressions inside aggregation
    Given an empty graph
    And having executed:
      """
      CREATE (andres {name: 'Andres'}),
             (michael {name: 'Michael'}),
             (peter {name: 'Peter'}),
             (bread {type: 'Bread'}),
             (veggies {type: 'Veggies'}),
             (meat {type: 'Meat'})
      CREATE (andres)-[:ATE {times: 10}]->(bread),
             (andres)-[:ATE {times: 8}]->(veggies),
             (michael)-[:ATE {times: 4}]->(veggies),
             (michael)-[:ATE {times: 6}]->(bread),
             (michael)-[:ATE {times: 9}]->(meat),
             (peter)-[:ATE {times: 7}]->(veggies),
             (peter)-[:ATE {times: 7}]->(bread),
             (peter)-[:ATE {times: 4}]->(meat)
      """
    When executing query:
      """
      MATCH (me)-[r1:ATE]->()<-[r2:ATE]-(you)
      WHERE me.name = 'Michael'
      WITH me, count(DISTINCT r1) AS H1, count(DISTINCT r2) AS H2, you
      MATCH (me)-[r1:ATE]->()<-[r2:ATE]-(you)
      RETURN me, you, sum((1 - abs(r1.times / H1 - r2.times / H2)) * (r1.times + r2.times) / (H1 + H2)) AS sum
      """
    Then the result should be:
      | me                  | you                | sum |
      | ({name: 'Michael'}) | ({name: 'Andres'}) | -7  |
      | ({name: 'Michael'}) | ({name: 'Peter'})  | 0   |
    And no side effects

  Scenario: Matching and disregarding output, then matching again
    Given an empty graph
    And having executed:
      """
      CREATE (andres {name: 'Andres'}),
             (michael {name: 'Michael'}),
             (peter {name: 'Peter'}),
             (bread {type: 'Bread'}),
             (veggies {type: 'Veggies'}),
             (meat {type: 'Meat'})
      CREATE (andres)-[:ATE {times: 10}]->(bread),
             (andres)-[:ATE {times: 8}]->(veggies),
             (michael)-[:ATE {times: 4}]->(veggies),
             (michael)-[:ATE {times: 6}]->(bread),
             (michael)-[:ATE {times: 9}]->(meat),
             (peter)-[:ATE {times: 7}]->(veggies),
             (peter)-[:ATE {times: 7}]->(bread),
             (peter)-[:ATE {times: 4}]->(meat)
      """
    When executing query:
      """
      MATCH ()-->()
      WITH 1 AS x
      MATCH ()-[r1]->()<--()
      RETURN sum(r1.times)
      """
    Then the result should be:
      | sum(r1.times) |
      | 776           |
    And no side effects

  Scenario: Returning a list property
    Given an empty graph
    And having executed:
      """
      CREATE ({foo: [1, 2, 3]})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
      """
    Then the result should be:
      | n                  |
      | ({foo: [1, 2, 3]}) |
    And no side effects

  Scenario: Returning a projected map
    Given an empty graph
    And having executed:
      """
      CREATE ({foo: [1, 2, 3]})
      """
    When executing query:
      """
      RETURN {a: 1, b: 'foo'}
      """
    Then the result should be:
      | {a: 1, b: 'foo'} |
      | {a: 1, b: 'foo'} |
    And no side effects

  Scenario: Returning an expression
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (a)
      RETURN exists(a.id), a IS NOT NULL
      """
    Then the result should be:
      | exists(a.id) | a IS NOT NULL |
      | false        | true          |
    And no side effects

  Scenario: Concatenating and returning the size of literal lists
    Given any graph
    When executing query:
      """
      RETURN size([[], []] + [[]]) AS l
      """
    Then the result should be:
      | l |
      | 3 |
    And no side effects

  Scenario: Returning nested expressions based on list property
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      SET n.array = [1, 2, 3, 4, 5]
      RETURN tail(tail(n.array))
      """
    Then the result should be:
      | tail(tail(n.array)) |
      | [3, 4, 5]           |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Limiting amount of rows when there are fewer left than the LIMIT argument
    Given an empty graph
    And having executed:
      """
      UNWIND range(0, 15) AS i
      CREATE ({count: i})
      """
    When executing query:
      """
      MATCH (a)
      RETURN a.count
        ORDER BY a.count
        SKIP 10
        LIMIT 10
      """
    Then the result should be, in order:
      | a.count |
      | 10      |
      | 11      |
      | 12      |
      | 13      |
      | 14      |
      | 15      |
    And no side effects

  Scenario: `substring()` with default second argument
    Given any graph
    When executing query:
      """
      RETURN substring('0123456789', 1) AS s
      """
    Then the result should be:
      | s           |
      | '123456789' |
    And no side effects

  Scenario: Returning all variables with ordering
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 1}), ({id: 10})
      """
    When executing query:
      """
      MATCH (n)
      RETURN *
        ORDER BY n.id
      """
    Then the result should be, in order:
      | n          |
      | ({id: 1})  |
      | ({id: 10}) |
    And no side effects

  Scenario: Using aliased DISTINCT expression in ORDER BY
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 1}), ({id: 10})
      """
    When executing query:
      """
      MATCH (n)
      RETURN DISTINCT n.id AS id
        ORDER BY id DESC
      """
    Then the result should be, in order:
      | id |
      | 10 |
      | 1  |
    And no side effects

  Scenario: Returned columns do not change from using ORDER BY
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 1}), ({id: 10})
      """
    When executing query:
      """
      MATCH (n)
      RETURN DISTINCT n
        ORDER BY n.id
      """
    Then the result should be, in order:
      | n          |
      | ({id: 1})  |
      | ({id: 10}) |
    And no side effects

  Scenario: Arithmetic expressions should propagate null values
    Given any graph
    When executing query:
      """
      RETURN 1 + (2 - (3 * (4 / (5 ^ (6 % null))))) AS a
      """
    Then the result should be:
      | a    |
      | null |
    And no side effects

  Scenario: Indexing into nested literal lists
    Given any graph
    When executing query:
      """
      RETURN [[1]][0][0]
      """
    Then the result should be:
      | [[1]][0][0] |
      | 1           |
    And no side effects

  Scenario: Aliasing expressions
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 42})
      """
    When executing query:
      """
      MATCH (a)
      RETURN a.id AS a, a.id
      """
    Then the result should be:
      | a  | a.id |
      | 42 | 42   |
    And no side effects

  Scenario: Projecting an arithmetic expression with aggregation
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 42})
      """
    When executing query:
      """
      MATCH (a)
      RETURN a, count(a) + 3
      """
    Then the result should be:
      | a          | count(a) + 3 |
      | ({id: 42}) | 4            |
    And no side effects

  Scenario: Multiple aliasing and backreferencing
    Given any graph
    When executing query:
      """
      CREATE (m {id: 0})
      WITH {first: m.id} AS m
      WITH {second: m.first} AS m
      RETURN m.second
      """
    Then the result should be:
      | m.second |
      | 0        |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Aggregating by a list property has a correct definition of equality
    Given an empty graph
    And having executed:
      """
      CREATE ({a: [1, 2, 3]}), ({a: [1, 2, 3]})
      """
    When executing query:
      """
      MATCH (a)
      WITH a.a AS a, count(*) AS count
      RETURN count
      """
    Then the result should be:
      | count |
      | 2     |
    And no side effects

  Scenario: Reusing variable names
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person), (b:Person), (m:Message {id: 10})
      CREATE (a)-[:LIKE {creationDate: 20160614}]->(m)-[:POSTED_BY]->(b)
      """
    When executing query:
      """
      MATCH (person:Person)<--(message)<-[like]-(:Person)
      WITH like.creationDate AS likeTime, person AS person
        ORDER BY likeTime, message.id
      WITH head(collect({likeTime: likeTime})) AS latestLike, person AS person
      RETURN latestLike.likeTime AS likeTime
        ORDER BY likeTime
      """
    Then the result should be, in order:
      | likeTime |
      | 20160614 |
    And no side effects

  Scenario: Concatenating lists of same type
    Given any graph
    When executing query:
      """
      RETURN [1, 10, 100] + [4, 5] AS foo
      """
    Then the result should be:
      | foo                |
      | [1, 10, 100, 4, 5] |
    And no side effects

  Scenario: Appending lists of same type
    Given any graph
    When executing query:
      """
      RETURN [false, true] + false AS foo
      """
    Then the result should be:
      | foo                  |
      | [false, true, false] |
    And no side effects

  Scenario: DISTINCT inside aggregation should work with lists in maps
    Given an empty graph
    And having executed:
      """
      CREATE ({list: ['A', 'B']}), ({list: ['A', 'B']})
      """
    When executing query:
      """
      MATCH (n)
      RETURN count(DISTINCT {foo: n.list}) AS count
      """
    Then the result should be:
      | count |
      | 1     |
    And no side effects

  Scenario: Handling DISTINCT with lists in maps
    Given an empty graph
    And having executed:
      """
      CREATE ({list: ['A', 'B']}), ({list: ['A', 'B']})
      """
    When executing query:
      """
      MATCH (n)
      WITH DISTINCT {foo: n.list} AS map
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And no side effects

  Scenario: DISTINCT inside aggregation should work with nested lists in maps
    Given an empty graph
    And having executed:
      """
      CREATE ({list: ['A', 'B']}), ({list: ['A', 'B']})
      """
    When executing query:
      """
      MATCH (n)
      RETURN count(DISTINCT {foo: [[n.list, n.list], [n.list, n.list]]}) AS count
      """
    Then the result should be:
      | count |
      | 1     |
    And no side effects

  Scenario: DISTINCT inside aggregation should work with nested lists of maps in maps
    Given an empty graph
    And having executed:
      """
      CREATE ({list: ['A', 'B']}), ({list: ['A', 'B']})
      """
    When executing query:
      """
      MATCH (n)
      RETURN count(DISTINCT {foo: [{bar: n.list}, {baz: {apa: n.list}}]}) AS count
      """
    Then the result should be:
      | count |
      | 1     |
    And no side effects
