# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

Feature: Subqueries
    Behaviour tests for memgraph CALL clause which contains a subquery

    Scenario: Subquery without bounded symbols
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label1)
            CALL {
                MATCH (n:Label1)-[:TYPE]->(m:Label2)
                RETURN m
            }
            RETURN m.prop;
            """
        Then the result should be:
            | m.prop |
            | 2      |

    Scenario: Subquery without bounded symbols and without match
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            CALL {
                MATCH (n:Label1)-[:TYPE]->(m:Label2)
                RETURN m
            }
            RETURN m.prop;
            """
        Then the result should be:
            | m.prop |
            | 2      |

    Scenario: Subquery returning primitive
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            CALL {
                MATCH (n:Label1)-[:TYPE]->(m:Label2)
                RETURN m.prop AS prop
            }
            RETURN prop;
            """
        Then the result should be:
            | prop     |
            | 2        |

    Scenario: Subquery returning 2 values
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label1)
            CALL {
                MATCH (m:Label1)-[:TYPE]->(o:Label2)
                RETURN m, o
            }
            RETURN m.prop, o.prop;
            """
        Then the result should be:
            | m.prop | o.prop |
            | 1      | 2      |

    Scenario: Subquery returning nothing because match did not find any results
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label3)
            CALL {
                MATCH (m:Label1)-[:TYPE]->(:Label2)
                RETURN m
            }
            RETURN m.prop;
            """
        Then the result should be empty

    Scenario: Subquery returning a multiple of results since we join elements from basic query and the subquery
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label1)
            CALL {
                MATCH (m)
                RETURN m
            }
            RETURN n.prop, m.prop
            ORDER BY n.prop, m.prop;
            """
        Then the result should be:
            | n.prop | m.prop |
            | 1      | 1      |
            | 1      | 2      |

    Scenario: Subquery returning a cartesian product
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n)
            CALL {
                MATCH (m)
                RETURN m
            }
            RETURN n.prop, m.prop
            ORDER BY n.prop, m.prop;
            """
        Then the result should be:
            | n.prop | m.prop |
            | 1      | 1      |
            | 1      | 2      |
            | 2      | 1      |
            | 2      | 2      |

    Scenario: Subquery with bounded symbols
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label1)
            CALL {
				        WITH n
                MATCH (n)-[:TYPE]->(m:Label2)
                RETURN m
            }
            RETURN m.prop;
            """
        Then the result should be:
            | m.prop |
            | 2      |

    Scenario: Subquery with invalid bounded symbols
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label1)
            CALL {
				        WITH o
                MATCH (o)-[:TYPE]->(m:Label2)
                RETURN m
            }
            RETURN m.prop;
            """
        Then an error should be raised

    Scenario: Subquery with an unbound variable
        Given an empty graph
        When executing query:
        """
        MATCH (node1)
        CALL {
            MATCH (node2)
            WHERE node1.property > 0
            return 1 as state
        }
        return 1
        """
        Then an error should be raised

    Scenario: Subquery returning primitive but not aliased
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label1)
            CALL {
                WITH n
                MATCH (n)-[:TYPE]->(m:Label2)
                RETURN m.prop
            }
            RETURN n;
            """
        Then an error should be raised

    Scenario: Subquery returning one primitive and others aliased
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label1)
            CALL {
                WITH n
                MATCH (o)-[:TYPE]->(m:Label2)
                RETURN m.prop, o
            }
            RETURN n;
            """
        Then an error should be raised

    Scenario: Subquery returning already declared variable in outer scope
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n:Label1), (m:Label2)
            CALL {
                WITH n
                MATCH (n:Label1)-[:TYPE]->(m:Label2)
                RETURN m
            }
            RETURN n;
            """
        Then an error should be raised

    Scenario: Subquery after subquery
        Given an empty graph
        And having executed
            """
            CREATE (:Label1 {prop: 1})-[:TYPE]->(:Label2 {prop: 2})
            """
        When executing query:
            """
            MATCH (n)
            CALL {
                MATCH (m)
                RETURN m
            }
            CALL {
                MATCH (o)
                RETURN o
            }
            RETURN n.prop, m.prop, o.prop
            ORDER BY n.prop, m.prop, o.prop;
            """
        Then the result should be:
            | n.prop | m.prop | o.prop |
            | 1      | 1      | 1      |
            | 1      | 1      | 2      |
            | 1      | 2      | 1      |
            | 1      | 2      | 2      |
            | 2      | 1      | 1      |
            | 2      | 1      | 2      |
            | 2      | 2      | 1      |
            | 2      | 2      | 2      |

    Scenario: Subquery with union
        Given an empty graph
        And having executed
            """
            CREATE (:Person {figure: "grandpa"})<-[:CHILD_OF]-(:Person {figure: "dad"})-[:PARENT_OF]->(:Person {figure: "child"})
            """
        When executing query:
            """
            MATCH (p:Person {figure: "dad"})
            CALL {
                WITH p
                OPTIONAL MATCH (p)-[:CHILD_OF]->(other:Person)
                RETURN other
              UNION
                WITH p
                OPTIONAL MATCH (p)-[:PARENT_OF]->(other:Person)
                RETURN other
            } RETURN DISTINCT p.figure, count(other) as cnt;
            """
        Then the result should be:
            | p.figure | cnt |
            | 'dad'    | 2   |

    Scenario: Subquery cloning nodes
        Given an empty graph
        And having executed
            """
            CREATE (:Person {name: "Alen"}), (:Person {name: "Bruce"})
            """
        When executing query:
            """
            MATCH (p:Person)
            CALL {
                WITH p
                UNWIND range (1, 3) AS i
                CREATE (n:Person {name: p.name})
                RETURN n
            }
            RETURN n;
            """
        Then the result should be:
            | n                         |
            | (:Person {name: 'Alen'})  |
            | (:Person {name: 'Alen'})  |
            | (:Person {name: 'Alen'})  |
            | (:Person {name: 'Bruce'}) |
            | (:Person {name: 'Bruce'}) |
            | (:Person {name: 'Bruce'}) |

    Scenario: Subquery in subquery
        Given an empty graph
        And having executed
            """
            CREATE (:Label {id: 1}), (:Label {id: 2})
            """
        When executing query:
            """
            MATCH (p:Label)
            CALL {
                MATCH (r:Label)
                CALL {
                    MATCH (s:Label)
                    RETURN s
                }
                RETURN r, s
            }
            RETURN p.id, r.id, s.id;
            """
        Then the result should be:
            | p.id | r.id | s.id |
            | 1    | 1    | 1    |
            | 1    | 1    | 2    |
            | 1    | 2    | 1    |
            | 1    | 2    | 2    |
            | 2    | 1    | 1    |
            | 2    | 1    | 2    |
            | 2    | 2    | 1    |
            | 2    | 2    | 2    |

    Scenario: Counter inside subquery
        Given an empty graph
        And having executed
            """
            CREATE (:Counter {count: 0})
            """
        When executing query:
            """
            UNWIND [0, 1, 2] AS x
            CALL {
                MATCH (n:Counter)
                SET n.count = n.count + 1
                RETURN n.count AS innerCount
            }
            WITH innerCount
            MATCH (n:Counter)
            RETURN innerCount, n.count AS totalCount
            """
        Then the result should be:
            | innerCount | totalCount |
            | 1          | 3          |
            | 2          | 3          |
            | 3          | 3          |

    Scenario: Advance command on multiple subqueries
        Given an empty graph
        When executing query:
            """
            CALL {
                CREATE (create_node:Movie {title: "Forrest Gump"})
            }
            CALL {
                MATCH (n) RETURN n
            }
            RETURN n.title AS title;
            """
        Then the result should be:
            | title          |
            | 'Forrest Gump' |

    Scenario: Advance command on multiple subqueries with manual accumulate
        Given an empty graph
        When executing query:
            """
            CALL {
                CREATE (create_node:Movie {title: "Forrest Gump"})
                RETURN create_node
            }
            WITH create_node
            CALL {
                MATCH (n) RETURN n
            }
            RETURN n.title AS title;
            """
        Then the result should be:
            | title          |
            | 'Forrest Gump' |
