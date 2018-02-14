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

Feature: CreateAcceptance

  Scenario: Create a single node with multiple labels
    Given an empty graph
    When executing query:
      """
      CREATE (:A:B:C:D)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 4 |

  Scenario: Combine MATCH and CREATE
    Given an empty graph
    And having executed:
      """
      CREATE (), ()
      """
    When executing query:
      """
      MATCH ()
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 2 |

  Scenario: Combine MATCH, WITH and CREATE
    Given an empty graph
    And having executed:
      """
      CREATE (), ()
      """
    When executing query:
      """
      MATCH ()
      CREATE ()
      WITH *
      MATCH ()
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 10 |

  Scenario: Newly-created nodes not visible to preceding MATCH
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH ()
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |

  Scenario: Create a single node with properties
    Given any graph
    When executing query:
      """
      CREATE (n {prop: 'foo'})
      RETURN n.prop AS p
      """
    Then the result should be:
      | p     |
      | 'foo' |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Creating a node with null properties should not return those properties
    Given any graph
    When executing query:
      """
      CREATE (n {id: 12, property: null})
      RETURN n.id AS id
      """
    Then the result should be:
      | id |
      | 12 |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Creating a relationship with null properties should not return those properties
    Given any graph
    When executing query:
      """
      CREATE ()-[r:X {id: 12, property: null}]->()
      RETURN r.id
      """
    Then the result should be:
      | r.id |
      | 12   |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +properties    | 1 |

  Scenario: Create a simple pattern
    Given any graph
    When executing query:
      """
      CREATE ()-[:R]->()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: Create a self loop
    Given an empty graph
    When executing query:
      """
      CREATE (root:R)-[:LINK]->(root)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 1 |
      | +relationships | 1 |
      | +labels        | 1 |

  Scenario: Create a self loop using MATCH
    Given an empty graph
    And having executed:
      """
      CREATE (:R)
      """
    When executing query:
      """
      MATCH (root:R)
      CREATE (root)-[:LINK]->(root)
      """
    Then the result should be empty
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Create nodes and relationships
    Given any graph
    When executing query:
      """
      CREATE (a), (b),
             (a)-[:R]->(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: Create a relationship with a property
    Given any graph
    When executing query:
      """
      CREATE ()-[:R {prop: 42}]->()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +properties    | 1 |

  Scenario: Create a relationship with the correct direction
    Given an empty graph
    And having executed:
      """
      CREATE (:X)
      CREATE (:Y)
      """
    When executing query:
      """
      MATCH (x:X), (y:Y)
      CREATE (x)<-[:TYPE]-(y)
      """
    Then the result should be empty
    And the side effects should be:
      | +relationships | 1 |
    When executing control query:
      """
      MATCH (x:X)<-[:TYPE]-(y:Y)
      RETURN x, y
      """
    Then the result should be:
      | x    |  y   |
      | (:X) | (:Y) |

  Scenario: Create a relationship and an end node from a matched starting node
    Given an empty graph
    And having executed:
      """
      CREATE (:Begin)
      """
    When executing query:
      """
      MATCH (x:Begin)
      CREATE (x)-[:TYPE]->(:End)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 1 |
      | +relationships | 1 |
      | +labels        | 1 |
    When executing control query:
      """
      MATCH (x:Begin)-[:TYPE]->()
      RETURN x
      """
    Then the result should be:
      | x        |
      | (:Begin) |

  Scenario: Create a single node after a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (), ()
      """
    When executing query:
      """
      MATCH ()
      CREATE ()
      WITH *
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 4 |

  Scenario: Create a relationship with a reversed direction
    Given an empty graph
    When executing query:
      """
      CREATE (:A)<-[:R]-(:B)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +labels        | 2 |
    When executing control query:
      """
      MATCH (a:A)<-[:R]-(b:B)
      RETURN a, b
      """
    Then the result should be:
      | a    | b    |
      | (:A) | (:B) |

  Scenario: Create a pattern with multiple hops
    Given an empty graph
    When executing query:
      """
      CREATE (:A)-[:R]->(:B)-[:R]->(:C)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 3 |
      | +relationships | 2 |
      | +labels        | 3 |
    When executing control query:
      """
      MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C)
      RETURN a, b, c
      """
    Then the result should be:
      | a    | b    | c    |
      | (:A) | (:B) | (:C) |

  Scenario: Create a pattern with multiple hops in the reverse direction
    Given an empty graph
    When executing query:
      """
      CREATE (:A)<-[:R]-(:B)<-[:R]-(:C)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 3 |
      | +relationships | 2 |
      | +labels        | 3 |
    When executing control query:
      """
      MATCH (a)<-[:R]-(b)<-[:R]-(c)
      RETURN a, b, c
      """
    Then the result should be:
      | a    | b    | c    |
      | (:A) | (:B) | (:C) |

  Scenario: Create a pattern with multiple hops in varying directions
    Given an empty graph
    When executing query:
      """
      CREATE (:A)-[:R]->(:B)<-[:R]-(:C)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 3 |
      | +relationships | 2 |
      | +labels        | 3 |
    When executing control query:
      """
      MATCH (a:A)-[r1:R]->(b:B)<-[r2:R]-(c:C)
      RETURN a, b, c
      """
    Then the result should be:
      | a    | b    | c    |
      | (:A) | (:B) | (:C) |

  Scenario: Create a pattern with multiple hops with multiple types and varying directions
    Given any graph
    When executing query:
      """
      CREATE ()-[:R1]->()<-[:R2]-()-[:R3]->()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 4 |
      | +relationships | 3 |
    When executing query:
      """
      MATCH ()-[r1:R1]->()<-[r2:R2]-()-[r3:R3]->()
      RETURN r1, r2, r3
      """
    Then the result should be:
      | r1    | r2    | r3    |
      | [:R1] | [:R2] | [:R3] |

  Scenario: Nodes are not created when aliases are applied to variable names
    Given an empty graph
    And having executed:
      """
      CREATE ({foo: 1})
      """
    When executing query:
      """
      MATCH (n)
      MATCH (m)
      WITH n AS a, m AS b
      CREATE (a)-[:T]->(b)
      RETURN a, b
      """
    Then the result should be:
      | a          | b          |
      | ({foo: 1}) | ({foo: 1}) |
    And the side effects should be:
      | +relationships | 1 |

  Scenario: Only a single node is created when an alias is applied to a variable name
    Given an empty graph
    And having executed:
      """
      CREATE (:X)
      """
    When executing query:
      """
      MATCH (n)
      WITH n AS a
      CREATE (a)-[:T]->()
      RETURN a
      """
    Then the result should be:
      | a    |
      | (:X) |
    And the side effects should be:
      | +nodes         | 1 |
      | +relationships | 1 |

  Scenario: Nodes are not created when aliases are applied to variable names multiple times
    Given an empty graph
    And having executed:
      """
      CREATE ({foo: 'A'})
      """
    When executing query:
      """
      MATCH (n)
      MATCH (m)
      WITH n AS a, m AS b
      CREATE (a)-[:T]->(b)
      WITH a AS x, b AS y
      CREATE (x)-[:T]->(y)
      RETURN x, y
      """
    Then the result should be:
      | x            | y            |
      | ({foo: 'A'}) | ({foo: 'A'}) |
    And the side effects should be:
      | +relationships | 2 |

  Scenario: Only a single node is created when an alias is applied to a variable name multiple times
    Given an empty graph
    And having executed:
      """
      CREATE ({foo: 5})
      """
    When executing query:
      """
      MATCH (n)
      WITH n AS a
      CREATE (a)-[:T]->()
      WITH a AS x
      CREATE (x)-[:T]->()
      RETURN x
      """
    Then the result should be:
      | x          |
      | ({foo: 5}) |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 2 |

  Scenario: A bound node should be recognized after projection with WITH + WITH
    Given any graph
    When executing query:
      """
      CREATE (a)
      WITH a
      WITH *
      CREATE (b)
      CREATE (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: A bound node should be recognized after projection with WITH + UNWIND
    Given any graph
    When executing query:
      """
      CREATE (a)
      WITH a
      UNWIND [0] AS i
      CREATE (b)
      CREATE (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: A bound node should be recognized after projection with WITH + MERGE node
    Given an empty graph
    When executing query:
      """
      CREATE (a)
      WITH a
      MERGE ()
      CREATE (b)
      CREATE (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: A bound node should be recognized after projection with WITH + MERGE pattern
    Given an empty graph
    When executing query:
      """
      CREATE (a)
      WITH a
      MERGE (x)
      MERGE (y)
      MERGE (x)-[:T]->(y)
      CREATE (b)
      CREATE (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 2 |

  Scenario: Fail when trying to create using an undirected relationship pattern
    Given any graph
    When executing query:
      """
      CREATE ({id: 2})-[r:KNOWS]-({id: 1})
      RETURN r
      """
    Then a SyntaxError should be raised at compile time: RequiresDirectedRelationship

  Scenario: Creating a pattern with multiple hops and changing directions
    Given an empty graph
    When executing query:
      """
      CREATE (:A)<-[:R1]-(:B)-[:R2]->(:C)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 3 |
      | +relationships | 2 |
      | +labels        | 3 |
    When executing control query:
      """
      MATCH (a:A)<-[r1:R1]-(b:B)-[r2:R2]->(c:C)
      RETURN *
      """
    Then the result should be:
      | a    | b    | c    | r1    | r2    |
      | (:A) | (:B) | (:C) | [:R1] | [:R2] |
