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

Feature: SetAcceptance

  Scenario: Setting a node property to null removes the existing property
    Given an empty graph
    And having executed:
      """
      CREATE (:A {property1: 23, property2: 46})
      """
    When executing query:
      """
      MATCH (n:A)
      SET n.property1 = null
      RETURN n
      """
    Then the result should be:
      | n                    |
      | (:A {property2: 46}) |
    And the side effects should be:
      | -properties | 1 |

  Scenario: Setting a relationship property to null removes the existing property
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:REL {property1: 12, property2: 24}]->()
      """
    When executing query:
      """
      MATCH ()-[r]->()
      SET r.property1 = null
      RETURN r
      """
    Then the result should be:
      | r                      |
      | [:REL {property2: 24}] |
    And the side effects should be:
      | -properties | 1 |

  Scenario: Set a property
    Given any graph
    And having executed:
      """
      CREATE (:A {name: 'Andres'})
      """
    When executing query:
      """
      MATCH (n:A)
      WHERE n.name = 'Andres'
      SET n.name = 'Michael'
      RETURN n
      """
    Then the result should be:
      | n                      |
      | (:A {name: 'Michael'}) |
    And the side effects should be:
      | +properties | 1 |
      | -properties | 1 |

  Scenario: Set a property to an expression
    Given an empty graph
    And having executed:
      """
      CREATE (:A {name: 'Andres'})
      """
    When executing query:
      """
      MATCH (n:A)
      WHERE n.name = 'Andres'
      SET n.name = n.name + ' was here'
      RETURN n
      """
    Then the result should be:
      | n                              |
      | (:A {name: 'Andres was here'}) |
    And the side effects should be:
      | +properties | 1 |
      | -properties | 1 |

  Scenario: Set a property by selecting the node using a simple expression
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n:A)
      SET (n).name = 'neo4j'
      RETURN n
      """
    Then the result should be:
      | n                    |
      | (:A {name: 'neo4j'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Set a property by selecting the relationship using a simple expression
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:REL]->()
      """
    When executing query:
      """
      MATCH ()-[r:REL]->()
      SET (r).name = 'neo4j'
      RETURN r
      """
    Then the result should be:
      | r                      |
      | [:REL {name: 'neo4j'}] |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Setting a property to null removes the property
    Given an empty graph
    And having executed:
      """
      CREATE (:A {name: 'Michael', age: 35})
      """
    When executing query:
      """
      MATCH (n)
      WHERE n.name = 'Michael'
      SET n.name = null
      RETURN n
      """
    Then the result should be:
      | n              |
      | (:A {age: 35}) |
    And the side effects should be:
      | -properties | 1 |

  Scenario: Add a label to a node
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n:A)
      SET n:Foo
      RETURN n
      """
    Then the result should be:
      | n        |
      | (:A:Foo) |
    And the side effects should be:
      | +labels | 1 |

  Scenario: Adding a list property
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n:A)
      SET n.x = [1, 2, 3]
      RETURN [i IN n.x | i / 2.0] AS x
      """
    Then the result should be:
      | x               |
      | [0.5, 1.0, 1.5] |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Concatenate elements onto a list property
    Given any graph
    When executing query:
      """
      CREATE (a {foo: [1, 2, 3]})
      SET a.foo = a.foo + [4, 5]
      RETURN a.foo
      """
    Then the result should be:
      | a.foo           |
      | [1, 2, 3, 4, 5] |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Concatenate elements in reverse onto a list property
    Given any graph
    When executing query:
      """
      CREATE (a {foo: [3, 4, 5]})
      SET a.foo = [1, 2] + a.foo
      RETURN a.foo
      """
    Then the result should be:
      | a.foo           |
      | [1, 2, 3, 4, 5] |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Overwrite values when using +=
    Given an empty graph
    And having executed:
      """
      CREATE (:X {foo: 'A', bar: 'B'})
      """
    When executing query:
      """
      MATCH (n:X {foo: 'A'})
      SET n += {bar: 'C'}
      RETURN n
      """
    Then the result should be:
      | n                         |
      | (:X {foo: 'A', bar: 'C'}) |
    And the side effects should be:
      | +properties | 1 |
      | -properties | 1 |

  Scenario: Retain old values when using +=
    Given an empty graph
    And having executed:
      """
      CREATE (:X {foo: 'A'})
      """
    When executing query:
      """
      MATCH (n:X {foo: 'A'})
      SET n += {bar: 'B'}
      RETURN n
      """
    Then the result should be:
      | n                         |
      | (:X {foo: 'A', bar: 'B'}) |
    And the side effects should be:
      | +properties | 1 |

  Scenario: Explicit null values in a map remove old values
    Given an empty graph
    And having executed:
      """
      CREATE (:X {foo: 'A', bar: 'B'})
      """
    When executing query:
      """
      MATCH (n:X {foo: 'A'})
      SET n += {foo: null}
      RETURN n
      """
    Then the result should be:
      | n               |
      | (:X {bar: 'B'}) |
    And the side effects should be:
      | -properties | 1 |

  Scenario: Non-existent values in a property map are removed with SET =
    Given an empty graph
    And having executed:
      """
      CREATE (:X {foo: 'A', bar: 'B'})
      """
    When executing query:
      """
      MATCH (n:X {foo: 'A'})
      SET n = {foo: 'B', baz: 'C'}
      RETURN n
      """
    Then the result should be:
      | n                         |
      | (:X {foo: 'B', baz: 'C'}) |
    And the side effects should be:
      | +properties | 2 |
      | -properties | 2 |
