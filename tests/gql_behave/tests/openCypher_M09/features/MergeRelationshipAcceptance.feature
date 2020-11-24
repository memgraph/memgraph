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

Feature: MergeRelationshipAcceptance

  Scenario: Creating a relationship
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE]->(b)
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Matching a relationship
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:TYPE]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE]->(b)
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And no side effects

  Scenario: Matching two relationships
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:TYPE]->(b)
      CREATE (a)-[:TYPE]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE]->(b)
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 2        |
    And no side effects

  Scenario: Filtering relationships
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:TYPE {name: 'r1'}]->(b)
      CREATE (a)-[:TYPE {name: 'r2'}]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE {name: 'r2'}]->(b)
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And no side effects

  Scenario: Creating relationship when all matches filtered out
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:TYPE {name: 'r1'}]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE {name: 'r2'}]->(b)
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And the side effects should be:
      | +relationships | 1 |
      | +properties    | 1 |

  Scenario: Matching incoming relationship
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (b)-[:TYPE]->(a)
      CREATE (a)-[:TYPE]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)<-[r:TYPE]-(b)
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And no side effects

  Scenario: Creating relationship with property
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE {name: 'Lola'}]->(b)
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And the side effects should be:
      | +relationships | 1 |
      | +properties    | 1 |

  Scenario: Using ON CREATE on a node
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[:KNOWS]->(b)
        ON CREATE SET b.created = 1
      """
    Then the result should be empty
    And the side effects should be:
      | +relationships | 1 |
      | +properties    | 1 |

  Scenario: Using ON CREATE on a relationship
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE]->(b)
        ON CREATE SET r.name = 'Lola'
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And the side effects should be:
      | +relationships | 1 |
      | +properties    | 1 |

  Scenario: Using ON MATCH on created node
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[:KNOWS]->(b)
        ON MATCH SET b.created = 1
      """
    Then the result should be empty
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Using ON MATCH on created relationship
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:KNOWS]->(b)
        ON MATCH SET r.created = 1
      """
    Then the result should be empty
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Using ON MATCH on a relationship
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:TYPE]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE]->(b)
        ON MATCH SET r.name = 'Lola'
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Using ON CREATE and ON MATCH
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {id: 1}), (b:B {id: 2})
      CREATE (a)-[:TYPE]->(b)
      CREATE (:A {id: 3}), (:B {id: 4})
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:TYPE]->(b)
        ON CREATE SET r.name = 'Lola'
        ON MATCH SET r.name = 'RUN'
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 4        |
    And the side effects should be:
      | +relationships | 3 |
      | +properties    | 4 |

  Scenario: Creating relationship using merged nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      """
    When executing query:
      """
      MERGE (a:A)
      MERGE (b:B)
      MERGE (a)-[:FOO]->(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Mixing MERGE with CREATE
    Given an empty graph
    When executing query:
      """
      CREATE (a:A), (b:B)
      MERGE (a)-[:KNOWS]->(b)
      CREATE (b)-[:KNOWS]->(c:C)
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And the side effects should be:
      | +nodes         | 3 |
      | +relationships | 2 |
      | +labels        | 3 |

  Scenario: Introduce named paths 1
    Given an empty graph
    When executing query:
      """
      MERGE (a {x: 1})
      MERGE (b {x: 2})
      MERGE p = (a)-[:R]->(b)
      RETURN p
      """
    Then the result should be:
      | p                         |
      | <({x: 1})-[:R]->({x: 2})> |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +properties    | 2 |

  Scenario: Introduce named paths 2
    Given an empty graph
    When executing query:
      """
      MERGE p = (a {x: 1})
      RETURN p
      """
    Then the result should be:
      | p          |
      | <({x: 1})> |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Use outgoing direction when unspecified
    Given an empty graph
    When executing query:
      """
      CREATE (a {id: 2}), (b {id: 1})
      MERGE (a)-[r:KNOWS]-(b)
      RETURN startNode(r).id AS s, endNode(r).id AS e
      """
    Then the result should be:
      | s | e |
      | 2 | 1 |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +properties    | 2 |

  Scenario: Match outgoing relationship when direction unspecified
    Given an empty graph
    And having executed:
      """
      CREATE (a {id: 1}), (b {id: 2})
      CREATE (a)-[:KNOWS]->(b)
      """
    When executing query:
      """
      MATCH (a {id: 2}), (b {id: 1})
      MERGE (a)-[r:KNOWS]-(b)
      RETURN r
      """
    Then the result should be:
      | r        |
      | [:KNOWS] |
    And no side effects

  Scenario: Match both incoming and outgoing relationships when direction unspecified
    Given an empty graph
    And having executed:
      """
      CREATE (a {id: 2}), (b {id: 1}), (c {id: 1}), (d {id: 2})
      CREATE (a)-[:KNOWS {name: 'ab'}]->(b)
      CREATE (c)-[:KNOWS {name: 'cd'}]->(d)
      """
    When executing query:
      """
      MATCH (a {id: 2})--(b {id: 1})
      MERGE (a)-[r:KNOWS]-(b)
      RETURN r
      """
    Then the result should be:
      | r                     |
      | [:KNOWS {name: 'ab'}] |
      | [:KNOWS {name: 'cd'}] |
    And no side effects

  Scenario: Fail when imposing new predicates on a variable that is already bound
    Given any graph
    When executing query:
      """
      CREATE (a:Foo)
      MERGE (a)-[r:KNOWS]->(a:Bar)
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: Using list properties via variable
    Given an empty graph
    When executing query:
      """
      CREATE (a:Foo), (b:Bar)
      WITH a, b
      UNWIND ['a,b', 'a,b'] AS str
      WITH a, b, split(str, ',') AS roles
      MERGE (a)-[r:FB {foobar: roles}]->(b)
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 2        |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +labels        | 2 |
      | +properties    | 1 |

  Scenario: Matching using list property
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:T {prop: [42, 43]}]->(b)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      MERGE (a)-[r:T {prop: [42, 43]}]->(b)
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 1        |
    And no side effects

  Scenario: Using bound variables from other updating clause
    Given an empty graph
    When executing query:
      """
      CREATE (a), (b)
      MERGE (a)-[:X]->(b)
      RETURN count(a)
      """
    Then the result should be:
      | count(a) |
      | 1        |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: UNWIND with multiple merges
    Given an empty graph
    When executing query:
      """
      UNWIND ['Keanu Reeves', 'Hugo Weaving', 'Carrie-Anne Moss', 'Laurence Fishburne'] AS actor
      MERGE (m:Movie {name: 'The Matrix'})
      MERGE (p:Person {name: actor})
      MERGE (p)-[:ACTED_IN]->(m)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 5 |
      | +relationships | 4 |
      | +labels        | 2 |
      | +properties    | 5 |

  Scenario: Do not match on deleted entities
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (b1:B {value: 0}), (b2:B {value: 1})
      CREATE (c1:C), (c2:C)
      CREATE (a)-[:REL]->(b1),
             (a)-[:REL]->(b2),
             (b1)-[:REL]->(c1),
             (b2)-[:REL]->(c2)
      """
    When executing query:
      """
      MATCH (a:A)-[ab]->(b:B)-[bc]->(c:C)
      DELETE ab, bc, b, c
      MERGE (newB:B {value: 1})
      MERGE (a)-[:REL]->(newB)
      MERGE (newC:C)
      MERGE (newB)-[:REL]->(newC)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | -nodes         | 4 |
      | +relationships | 2 |
      | -relationships | 4 |
      | +properties    | 1 |
      | -properties    | 2 |

  Scenario: Do not match on deleted relationships
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:T {name: 'rel1'}]->(b),
             (a)-[:T {name: 'rel2'}]->(b)
      """
    When executing query:
      """
      MATCH (a)-[t:T]->(b)
      DELETE t
      MERGE (a)-[t2:T {name: 'rel3'}]->(b)
      RETURN t2.name
      """
    Then the result should be:
      | t2.name |
      | 'rel3'  |
      | 'rel3'  |
    And the side effects should be:
      | +relationships | 1 |
      | -relationships | 2 |
      | +properties    | 1 |
      | -properties    | 2 |

  Scenario: Aliasing of existing nodes 1
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      MATCH (n)
      MATCH (m)
      WITH n AS a, m AS b
      MERGE (a)-[r:T]->(b)
      RETURN a.id AS a, b.id AS b
      """
    Then the result should be:
      | a | b |
      | 0 | 0 |
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Aliasing of existing nodes 2
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      MATCH (n)
      WITH n AS a, n AS b
      MERGE (a)-[r:T]->(b)
      RETURN a.id AS a
      """
    Then the result should be:
      | a |
      | 0 |
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Double aliasing of existing nodes 1
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      MATCH (n)
      MATCH (m)
      WITH n AS a, m AS b
      MERGE (a)-[:T]->(b)
      WITH a AS x, b AS y
      MERGE (a)
      MERGE (b)
      MERGE (a)-[:T]->(b)
      RETURN x.id AS x, y.id AS y
      """
    Then the result should be:
      | x | y |
      | 0 | 0 |
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Double aliasing of existing nodes 2
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      MATCH (n)
      WITH n AS a
      MERGE (c)
      MERGE (a)-[:T]->(c)
      WITH a AS x
      MERGE (c)
      MERGE (x)-[:T]->(c)
      RETURN x.id AS x
      """
    Then the result should be:
      | x |
      | 0 |
    And the side effects should be:
      | +relationships | 1 |
