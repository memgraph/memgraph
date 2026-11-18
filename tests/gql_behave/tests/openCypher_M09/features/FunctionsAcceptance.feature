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

Feature: FunctionsAcceptance

  Scenario: Run coalesce
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'Emil Eifrem', title: 'CEO'}), ({name: 'Nobody'})
      """
    When executing query:
      """
      MATCH (a)
      RETURN coalesce(a.title, a.name)
      """
    Then the result should be:
      | coalesce(a.title, a.name) |
      | 'CEO'                     |
      | 'Nobody'                  |
    And no side effects

  Scenario: Functions should return null if they get path containing unbound
    Given any graph
    When executing query:
      """
      WITH null AS a
      OPTIONAL MATCH p = (a)-[r]->()
      RETURN length(nodes(p)), type(r), nodes(p), relationships(p)
      """
    Then the result should be:
      | length(nodes(p)) | type(r) | nodes(p) | relationships(p) |
      | null             | null    | null     | null             |
    And no side effects

  Scenario: `split()`
    Given any graph
    When executing query:
      """
      UNWIND split('one1two', '1') AS item
      RETURN count(item) AS item
      """
    Then the result should be:
      | item |
      | 2    |
    And no side effects

  Scenario: `properties()` on a node
    Given an empty graph
    And having executed:
      """
      CREATE (n:Person {name: 'Popeye', level: 9001})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN properties(p) AS m
      """
    Then the result should be:
      | m                             |
      | {name: 'Popeye', level: 9001} |
    And no side effects

  Scenario: `properties()` on a relationship
    Given an empty graph
    And having executed:
      """
      CREATE (n)-[:R {name: 'Popeye', level: 9001}]->(n)
      """
    When executing query:
      """
      MATCH ()-[r:R]->()
      RETURN properties(r) AS m
      """
    Then the result should be:
      | m                             |
      | {name: 'Popeye', level: 9001} |
    And no side effects

  Scenario: `properties()` on a map
    Given any graph
    When executing query:
      """
      RETURN properties({name: 'Popeye', level: 9001}) AS m
      """
    Then the result should be:
      | m                             |
      | {name: 'Popeye', level: 9001} |
    And no side effects

  Scenario: `properties()` failing on an integer literal
    Given any graph
    When executing query:
      """
      RETURN properties(1)
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: `properties()` failing on a string literal
    Given any graph
    When executing query:
      """
      RETURN properties('Cypher')
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: `properties()` failing on a list of booleans
    Given any graph
    When executing query:
      """
      RETURN properties([true, false])
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: `properties()` on null
    Given any graph
    When executing query:
      """
      RETURN properties(null)
      """
    Then the result should be:
      | properties(null) |
      | null             |
    And no side effects

  Scenario: `reverse()`
    Given any graph
    When executing query:
      """
      RETURN reverse('raksO')
      """
    Then the result should be:
      | reverse('raksO') |
      | 'Oskar'          |
    And no side effects

  Scenario: `exists()` with dynamic property lookup
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {prop: 'foo'}),
             (:Person)
      """
    When executing query:
      """
      MATCH (n:Person)
      WHERE exists(n['prop'])
      RETURN n
      """
    Then the result should be:
      | n                       |
      | (:Person {prop: 'foo'}) |
    And no side effects

  Scenario Outline: `exists()` with literal maps
    Given any graph
    When executing query:
      """
      WITH <map> AS map
      RETURN exists(map.name) AS result
      """
    Then the result should be:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | map                             | result |
      | {name: 'Mats', name2: 'Pontus'} | true   |
      | {name: null}                    | false  |
      | {notName: 0, notName2: null}    | false  |

  Scenario Outline: IS NOT NULL with literal maps
    Given any graph
    When executing query:
      """
      WITH <map> AS map
      RETURN map.name IS NOT NULL
      """
    Then the result should be:
      | map.name IS NOT NULL |
      | <result>             |
    And no side effects

    Examples:
      | map                             | result |
      | {name: 'Mats', name2: 'Pontus'} | true   |
      | {name: null}                    | false  |
      | {notName: 0, notName2: null}    | false  |

  Scenario Outline: `percentileDisc()`
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 10.0}),
             ({prop: 20.0}),
             ({prop: 30.0})
      """
    And parameters are:
      | percentile | <p> |
    When executing query:
      """
      MATCH (n)
      RETURN percentileDisc(n.prop, $percentile) AS p
      """
    Then the result should be:
      | p        |
      | <result> |
    And no side effects

    Examples:
      | p   | result |
      | 0.0 | 10.0   |
      | 0.5 | 20.0   |
      | 1.0 | 30.0   |

  Scenario Outline: `percentileCont()`
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 10.0}),
             ({prop: 20.0}),
             ({prop: 30.0})
      """
    And parameters are:
      | percentile | <p> |
    When executing query:
      """
      MATCH (n)
      RETURN percentileCont(n.prop, $percentile) AS p
      """
    Then the result should be:
      | p        |
      | <result> |
    And no side effects

    Examples:
      | p   | result |
      | 0.0 | 10.0   |
      | 0.5 | 20.0   |
      | 1.0 | 30.0   |

  Scenario Outline: `percentileCont()` failing on bad arguments
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 10.0})
      """
    And parameters are:
      | param | <percentile> |
    When executing query:
      """
      MATCH (n)
      RETURN percentileCont(n.prop, $param)
      """
    Then a ArgumentError should be raised at runtime: NumberOutOfRange

    Examples:
      | percentile |
      | 1000       |
      | -1         |
      | 1.1        |

  Scenario Outline: `percentileDisc()` failing on bad arguments
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 10.0})
      """
    And parameters are:
      | param | <percentile> |
    When executing query:
      """
      MATCH (n)
      RETURN percentileDisc(n.prop, $param)
      """
    Then a ArgumentError should be raised at runtime: NumberOutOfRange

    Examples:
      | percentile |
      | 1000       |
      | -1         |
      | 1.1        |

  Scenario: `percentileDisc()` failing in more involved query
    Given an empty graph
    And having executed:
      """
      UNWIND range(0, 10) AS i
      CREATE (s:S)
      WITH s, i
      UNWIND range(0, i) AS j
      CREATE (s)-[:REL]->()
      """
    When executing query:
      """
      MATCH (n:S)
      WITH n, size([(n)-->() | 1]) AS deg
      WHERE deg > 2
      WITH deg
      LIMIT 100
      RETURN percentileDisc(0.90, deg), deg
      """
    Then a ArgumentError should be raised at runtime: NumberOutOfRange

  Scenario: `type()`
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH ()-[r]->()
      RETURN type(r)
      """
    Then the result should be:
      | type(r) |
      | 'T'     |
    And no side effects

  Scenario: `type()` on two relationships
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T1]->()-[:T2]->()
      """
    When executing query:
      """
      MATCH ()-[r1]->()-[r2]->()
      RETURN type(r1), type(r2)
      """
    Then the result should be:
      | type(r1) | type(r2) |
      | 'T1'     | 'T2'     |
    And no side effects

  Scenario: `type()` on null relationship
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (a)
      OPTIONAL MATCH (a)-[r:NOT_THERE]->()
      RETURN type(r)
      """
    Then the result should be:
      | type(r) |
      | null    |
    And no side effects

  Scenario: `type()` on mixed null and non-null relationships
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH (a)
      OPTIONAL MATCH (a)-[r:T]->()
      RETURN type(r)
      """
    Then the result should be:
      | type(r) |
      | 'T'     |
      | null    |
    And no side effects

  Scenario: `type()` handling Any type
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH (a)-[r]->()
      WITH [r, 1] AS list
      RETURN type(list[0])
      """
    Then the result should be:
      | type(list[0]) |
      | 'T'           |
    And no side effects

  Scenario Outline: `type()` failing on invalid arguments
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH p = (n)-[r:T]->()
      RETURN [x IN [r, <invalid>] | type(x) ] AS list
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

    Examples:
      | invalid |
      | 0       |
      | 1.0     |
      | true    |
      | ''      |
      | []      |

  Scenario: `labels()` should accept type Any
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar)
      """
    When executing query:
      """
      MATCH (a)
      WITH [a, 1] AS list
      RETURN labels(list[0]) AS l
      """
    Then the result should be (ignoring element order for lists):
      | l              |
      | ['Foo']        |
      | ['Foo', 'Bar'] |
    And no side effects

  Scenario: `labels()` failing on a path
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar)
      """
    When executing query:
      """
      MATCH p = (a)
      RETURN labels(p) AS l
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: `labels()` failing on invalid arguments
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar)
      """
    When executing query:
      """
      MATCH (a)
      WITH [a, 1] AS list
      RETURN labels(list[1]) AS l
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

  Scenario: `exists()` is case insensitive
    Given an empty graph
    And having executed:
      """
      CREATE (a:X {prop: 42}), (:X)
      """
    When executing query:
      """
      MATCH (n:X)
      RETURN n, EXIsTS(n.prop) AS b
      """
    Then the result should be:
      | n               | b     |
      | (:X {prop: 42}) | true  |
      | (:X)            | false |
    And no side effects
