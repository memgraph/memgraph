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

Feature: RemoveAcceptance

  Scenario: Should ignore nulls
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 42})
      """
    When executing query:
      """
      MATCH (n)
      OPTIONAL MATCH (n)-[r]->()
      REMOVE r.prop
      RETURN n
      """
    Then the result should be:
      | n            |
      | ({prop: 42}) |
    And no side effects

  Scenario: Remove a single label
    Given an empty graph
    And having executed:
      """
      CREATE (:L {prop: 42})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:L
      RETURN n.prop
      """
    Then the result should be:
      | n.prop |
      | 42     |
    And the side effects should be:
      | -labels | 1 |

  Scenario: Remove multiple labels
    Given an empty graph
    And having executed:
      """
      CREATE (:L1:L2:L3 {prop: 42})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:L1:L3
      RETURN labels(n)
      """
    Then the result should be:
      | labels(n) |
      | ['L2']    |
    And the side effects should be:
      | -labels | 2 |

  Scenario: Remove a single node property
    Given an empty graph
    And having executed:
      """
      CREATE (:L {prop: 42})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n.prop
      RETURN exists(n.prop) AS still_there
      """
    Then the result should be:
      | still_there |
      | false       |
    And the side effects should be:
      | -properties | 1 |

  Scenario: Remove multiple node properties
    Given an empty graph
    And having executed:
      """
      CREATE (:L {prop: 42, a: 'a', b: 'B'})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n.prop, n.a
      RETURN size(keys(n)) AS props
      """
    Then the result should be:
      | props |
      | 1     |
    And the side effects should be:
      | -properties | 2 |

  Scenario: Remove a single relationship property
    Given an empty graph
    And having executed:
      """
      CREATE (a), (b), (a)-[:X {prop: 42}]->(b)
      """
    When executing query:
      """
      MATCH ()-[r]->()
      REMOVE r.prop
      RETURN exists(r.prop) AS still_there
      """
    Then the result should be:
      | still_there |
      | false       |
    And the side effects should be:
      | -properties | 1 |

  Scenario: Remove multiple relationship properties
    Given an empty graph
    And having executed:
      """
      CREATE (a), (b), (a)-[:X {prop: 42, a: 'a', b: 'B'}]->(b)
      """
    When executing query:
      """
      MATCH ()-[r]->()
      REMOVE r.prop, r.a
      RETURN size(keys(r)) AS props
      """
    Then the result should be:
      | props |
      | 1     |
    And the side effects should be:
      | -properties | 2 |

  Scenario: Remove a missing property should be a valid operation
    Given an empty graph
    And having executed:
      """
      CREATE (), (), ()
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n.prop
      RETURN sum(size(keys(n))) AS totalNumberOfProps
      """
    Then the result should be:
      | totalNumberOfProps |
      | 0                  |
    And no side effects
