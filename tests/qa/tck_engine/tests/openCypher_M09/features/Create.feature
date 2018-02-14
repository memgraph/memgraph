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

Feature: Create

  Scenario: Creating a node
    Given any graph
    When executing query:
      """
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 1 |

  Scenario: Creating two nodes
    Given any graph
    When executing query:
      """
      CREATE (), ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 2 |

  Scenario: Creating two nodes and a relationship
    Given any graph
    When executing query:
      """
      CREATE ()-[:TYPE]->()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: Creating a node with a label
    Given an empty graph
    When executing query:
      """
      CREATE (:Label)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 1 |

  Scenario: Creating a node with a property
    Given any graph
    When executing query:
      """
      CREATE ({created: true})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
