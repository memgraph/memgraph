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

Feature: DeleteAcceptance

  Scenario: Delete nodes
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      DELETE n
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes | 1 |

  Scenario: Detach delete node
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      DETACH DELETE n
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes | 1 |

  Scenario: Delete relationships
    Given an empty graph
    And having executed:
      """
      UNWIND range(0, 2) AS i
      CREATE ()-[:R]->()
      """
    When executing query:
      """
      MATCH ()-[r]-()
      DELETE r
      """
    Then the result should be empty
    And the side effects should be:
      | -relationships | 3 |

  Scenario: Deleting connected nodes
    Given an empty graph
    And having executed:
      """
      CREATE (x:X)
      CREATE (x)-[:R]->()
      CREATE (x)-[:R]->()
      CREATE (x)-[:R]->()
      """
    When executing query:
      """
      MATCH (n:X)
      DELETE n
      """
    Then a ConstraintVerificationFailed should be raised at runtime: DeleteConnectedNode

  Scenario: Detach deleting connected nodes and relationships
    Given an empty graph
    And having executed:
      """
      CREATE (x:X)
      CREATE (x)-[:R]->()
      CREATE (x)-[:R]->()
      CREATE (x)-[:R]->()
      """
    When executing query:
      """
      MATCH (n:X)
      DETACH DELETE n
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes         | 1 |
      | -relationships | 3 |
      | -labels        | 1 |

  Scenario: Undirected expand followed by delete and count
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:R]->()
      """
    When executing query:
      """
      MATCH (a)-[r]-(b)
      DELETE r, a, b
      RETURN count(*) AS c
      """
    Then the result should be:
      | c |
      | 2 |
    And the side effects should be:
      | -nodes         | 2 |
      | -relationships | 1 |

  Scenario: Undirected variable length expand followed by delete and count
    Given an empty graph
    And having executed:
      """
      CREATE (n1), (n2), (n3)
      CREATE (n1)-[:R]->(n2)
      CREATE (n2)-[:R]->(n3)
      """
    When executing query:
      """
      MATCH (a)-[*]-(b)
      DETACH DELETE a, b
      RETURN count(*) AS c
      """
    Then the result should be:
      | c |
      | 6 |
    And the side effects should be:
      | -nodes         | 3 |
      | -relationships | 2 |

  Scenario: Create and delete in same query
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH ()
      CREATE (n)
      DELETE n
      """
    Then the result should be empty
    And no side effects

  Scenario: Delete optionally matched relationship
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      OPTIONAL MATCH (n)-[r]-()
      DELETE n, r
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes | 1 |

  Scenario: Delete on null node
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n)
      DELETE n
      """
    Then the result should be empty
    And no side effects

  Scenario: Detach delete on null node
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n)
      DETACH DELETE n
      """
    Then the result should be empty
    And no side effects

  Scenario: Delete on null path
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH p = ()-->()
      DETACH DELETE p
      """
    Then the result should be empty
    And no side effects

  Scenario: Delete node from a list
    Given an empty graph
    And having executed:
      """
      CREATE (u:User)
      CREATE (u)-[:FRIEND]->()
      CREATE (u)-[:FRIEND]->()
      CREATE (u)-[:FRIEND]->()
      CREATE (u)-[:FRIEND]->()
      """
    And parameters are:
      | friendIndex | 1 |
    When executing query:
      """
      MATCH (:User)-[:FRIEND]->(n)
      WITH collect(n) AS friends
      DETACH DELETE friends[$friendIndex]
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes         | 1 |
      | -relationships | 1 |

  Scenario: Delete relationship from a list
    Given an empty graph
    And having executed:
      """
      CREATE (u:User)
      CREATE (u)-[:FRIEND]->()
      CREATE (u)-[:FRIEND]->()
      CREATE (u)-[:FRIEND]->()
      CREATE (u)-[:FRIEND]->()
      """
    And parameters are:
      | friendIndex | 1 |
    When executing query:
      """
      MATCH (:User)-[r:FRIEND]->()
      WITH collect(r) AS friendships
      DETACH DELETE friendships[$friendIndex]
      """
    Then the result should be empty
    And the side effects should be:
      | -relationships | 1 |

  Scenario: Delete nodes from a map
    Given an empty graph
    And having executed:
      """
      CREATE (:User), (:User)
      """
    When executing query:
      """
      MATCH (u:User)
      WITH {key: u} AS nodes
      DELETE nodes.key
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes  | 2 |
      | -labels | 1 |

  Scenario: Delete relationships from a map
    Given an empty graph
    And having executed:
      """
      CREATE (a:User), (b:User)
      CREATE (a)-[:R]->(b)
      CREATE (b)-[:R]->(a)
      """
    When executing query:
      """
      MATCH (:User)-[r]->(:User)
      WITH {key: r} AS rels
      DELETE rels.key
      """
    Then the result should be empty
    And the side effects should be:
      | -relationships | 2 |

  Scenario: Detach delete nodes from nested map/list
    Given an empty graph
    And having executed:
      """
      CREATE (a:User), (b:User)
      CREATE (a)-[:R]->(b)
      CREATE (b)-[:R]->(a)
      """
    When executing query:
      """
      MATCH (u:User)
      WITH {key: collect(u)} AS nodeMap
      DETACH DELETE nodeMap.key[0]
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes         | 1 |
      | -relationships | 2 |

  Scenario: Delete relationships from nested map/list
    Given an empty graph
    And having executed:
      """
      CREATE (a:User), (b:User)
      CREATE (a)-[:R]->(b)
      CREATE (b)-[:R]->(a)
      """
    When executing query:
      """
      MATCH (:User)-[r]->(:User)
      WITH {key: {key: collect(r)}} AS rels
      DELETE rels.key.key[0]
      """
    Then the result should be empty
    And the side effects should be:
      | -relationships | 1 |

  Scenario: Delete paths from nested map/list
    Given an empty graph
    And having executed:
      """
      CREATE (a:User), (b:User)
      CREATE (a)-[:R]->(b)
      CREATE (b)-[:R]->(a)
      """
    When executing query:
      """
      MATCH p = (:User)-[r]->(:User)
      WITH {key: collect(p)} AS pathColls
      DELETE pathColls.key[0], pathColls.key[1]
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes         | 2 |
      | -relationships | 2 |
      | -labels        | 1 |

  Scenario: Delete relationship with bidirectional matching
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T {id: 42}]->()
      """
    When executing query:
      """
      MATCH p = ()-[r:T]-()
      WHERE r.id = 42
      DELETE r
      """
    Then the result should be empty
    And the side effects should be:
      | -relationships | 1 |
      | -properties    | 1 |
