# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

Feature: Hops Limit

    Scenario: Test hops limit DFS test01 - partial results
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *]->(e) RETURN p
            """
        Then the result should be:
            | p                                                                                                                           |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})>                                                                     |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})>                                   |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})-[:CONNECTED]->(:Node {name: 'H'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'E'})>                                   |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})>                                                                     |


    Scenario: Test hops limit DFS test02 - partial results
        Given graph "simple_binary_tree"
        And with new index :Node(name)
        When executing query:
            """
            USING HOPS LIMIT 2 MATCH p=(a:Node {name: 'A'})-[:CONNECTED *]->(d:Node {name: 'D'}) return p
            """
        Then the result should be empty


    Scenario: Test hops limit DFS test03 - partial results
        Given graph "simple_binary_tree"
        And with new index :Node(name)
        When executing query:
            """
            USING HOPS LIMIT 1 MATCH p=(a:Node {name: 'A'})-[:CONNECTED *]->(d:Node {name: 'D'}) return p
            """
        Then the result should be empty



    Scenario: Test hops limit DFS both directions - partial results
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *]-(e) RETURN p
            """
        Then the result should be:
            | p                                                                                         |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})>                                   |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'E'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})>                                   |

    Scenario: Test hops limit BFS test01 - partial results
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *BFS]->(e) RETURN p
            """
        Then the result should be:
            | p                                                                                         |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})>                                   |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})>                                   |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})-[:CONNECTED]->(:Node {name: 'G'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})-[:CONNECTED]->(:Node {name: 'F'})> |


    Scenario: Test hops limit BFS test02 - partial results
        Given graph "simple_binary_tree"
        And with new index :Node(name)
        When executing query:
            """
            USING HOPS LIMIT 3 MATCH p=(a:Node {name: 'A'})-[:CONNECTED *BFS]->(d:Node {name: 'D'}) return p
            """
        Then the result should be:
            | p                                                                                         |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})> |


    Scenario: Test hops limit BFS test03 - partial results
        Given graph "simple_binary_tree"
        And with new index :Node(name)
        When executing query:
            """
            USING HOPS LIMIT 2 MATCH p=(a:Node {name: 'A'})-[:CONNECTED *BFS]->(d:Node {name: 'D'}) return p
            """
            | p                                                                                    |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'B'})>                                  |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'B'})-[:CONNECTED]->(:Node{name:'D'})>  |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'B'})-[:CONNECTED]->(:Node{name:'E'})>  |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'C'})>                                  |


    Scenario: Test hops limit BFS both directions - partial results
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *BFS]-(e) RETURN p
            """
        Then the result should be:
            | p                                                                                   |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'C'})>                                 |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'B'})>                                 |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'C'})-[:CONNECTED]->(:Node{name:'G'})> |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'C'})-[:CONNECTED]->(:Node{name:'F'})> |


    Scenario: Test simple expand - partial results
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED]->(b) RETURN p
            """
        Then the result should be:
            | p                                                       |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})> |
            | <(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})> |
            | <(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'E'})> |
            | <(:Node {name: 'C'})-[:CONNECTED]->(:Node {name: 'F'})> |


    Scenario: Test simple expand both directions - partial results
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED]-(b) RETURN p
            """
        Then the result should be:
            | p                                                   |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'B'})> |
            | <(:Node{name:'A'})-[:CONNECTED]->(:Node{name:'C'})> |
            | <(:Node{name:'B'})<-[:CONNECTED]-(:Node{name:'A'})> |
            | <(:Node{name:'B'})-[:CONNECTED]->(:Node{name:'D'})> |
            | <(:Node{name:'B'})-[:CONNECTED]->(:Node{name:'E'})> |


    Scenario: Test hops limit flag with partial results disabled
        Given an empty graph
        And having executed:
            """
            CREATE (a)-[:CONNECTED]->(b)
            CREATE (a)-[:CONNECTED]->(c)
            CREATE (a)-[:CONNECTED]->(d)
            CREATE (a)-[:CONNECTED]->(e)
            CREATE (a)-[:CONNECTED]->(f)
            """
        And having executed:
            """
            SET DATABASE SETTING 'hops_limit_partial_results' TO 'false'
            """
        When executing query:
            """
            USING HOPS LIMIT 2 MATCH p=(a)-[:CONNECTED *]->(e) SET e:Test RETURN p
            """
        Then an error should be raised


    Scenario: Test hops limit flag with partial results enabled
        Given an empty graph
        And having executed:
            """
            CREATE  (a:Node {name: 'A'}),
                    (b:Node {name: 'B'}),
                    (c:Node {name: 'C'}),
                    (d:Node {name: 'D'}),
                    (e:Node {name: 'E'}),
                    (f:Node {name: 'F'})

            CREATE  (a)-[:CONNECTED]->(b),
                    (a)-[:CONNECTED]->(c),
                    (a)-[:CONNECTED]->(d),
                    (a)-[:CONNECTED]->(e),
                    (a)-[:CONNECTED]->(f)
            """
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *]->(e) RETURN p
            """
        Then the result should be:
            | p                                                       |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'D'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'E'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'F'})> |

    Scenario: Test retrieving hops limit counter
        Given an empty graph
        And having executed:
            """
            UNWIND range(1, 100) as x CREATE ()-[:NEXT]->()
            """
        When executing query:
            """
            USING HOPS LIMIT 100 CALL { MATCH (a)-[r]->(b) with a, r, b LIMIT 50 return count(*) as cnt } RETURN getHopsCounter() as counter;
            """
        Then the result should be:
            | counter |
            | 50      |

    Scenario: Test retrieving hops limit counter without limit set is also active
        Given an empty graph
        And having executed:
            """
            UNWIND range(1, 100) as x CREATE ()-[:NEXT]->()
            """
        When executing query:
            """
            CALL { MATCH (a)-[r]->(b) with a, r, b LIMIT 50 return count(*) as cnt } RETURN getHopsCounter() as counter;
            """
        Then the result should be:
            | counter |
            | 50      |

    Scenario: Test retrieving hops limit without traversing
        Given an empty graph
        And having executed:
            """
            UNWIND range(1, 100) as x CREATE ()-[:NEXT]->()
            """
        When executing query:
            """
            RETURN getHopsCounter() as counter;
            """
        Then the result should be:
            | counter |
            | 0       |

    Scenario: Test retrieving hops limit without traversing 2
        Given an empty graph
        And having executed:
            """
            UNWIND range(1, 100) as x CREATE ()-[:NEXT]->()
            """
        When executing query:
            """
            MATCH (n) with count(n) as cnt RETURN getHopsCounter() as counter;
            """
        Then the result should be:
            | counter |
            | 0       |

    # Unlimited, the three shortest are [0, 1, 7], [0, 1, 5, 4, 7] and [0, 6, 2, 5, 1, 7]. The limit cuts a
    # spur search short before it finds the 4-hop path, so serving the 5-hop path second would be wrong.
    Scenario: KSHORTEST under a hops limit serves only paths it can prove are the shortest
        Given an empty graph
        And having executed:
            """
            UNWIND range(0, 7) AS i CREATE (:V {id: i})
            """
        And having executed:
            """
            UNWIND [[0, 1], [0, 6], [1, 0], [1, 2], [1, 5], [1, 7], [2, 5], [3, 6], [4, 5], [4, 7], [5, 1], [5, 2], [5, 4], [6, 2], [7, 1], [7, 5]] AS e
            MATCH (a:V {id: e[0]}), (b:V {id: e[1]}) CREATE (a)-[:R]->(b)
            """
        And having executed:
            """
            SET DATABASE SETTING 'hops_limit_partial_results' TO 'true'
            """
        When executing query:
            """
            USING HOPS LIMIT 10 MATCH (a:V {id: 0}), (b:V {id: 7}) WITH a, b
            MATCH p=(a)-[:R *KSHORTEST|3]->(b) RETURN [x IN nodes(p) | x.id] AS path
            """
        Then the result should be:
            | path      |
            | [0, 1, 7] |

    # The limit runs out in the first search, after it has found the shortest path.
    Scenario: KSHORTEST under a hops limit serves the shortest path its first search found
        Given an empty graph
        And having executed:
            """
            UNWIND range(0, 7) AS i CREATE (:V {id: i})
            """
        And having executed:
            """
            UNWIND [[0, 1], [0, 6], [1, 0], [1, 2], [1, 5], [1, 7], [2, 5], [3, 6], [4, 5], [4, 7], [5, 1], [5, 2], [5, 4], [6, 2], [7, 1], [7, 5]] AS e
            MATCH (a:V {id: e[0]}), (b:V {id: e[1]}) CREATE (a)-[:R]->(b)
            """
        And having executed:
            """
            SET DATABASE SETTING 'hops_limit_partial_results' TO 'true'
            """
        When executing query:
            """
            USING HOPS LIMIT 3 MATCH (a:V {id: 0}), (b:V {id: 7}) WITH a, b
            MATCH p=(a)-[:R *KSHORTEST|3]->(b) RETURN [x IN nodes(p) | x.id] AS path
            """
        Then the result should be:
            | path      |
            | [0, 1, 7] |

    # S->T costs 10 and S->B->T costs 2. The limit cuts the search short after it has queued the
    # expensive route, so serving that route first would be wrong.
    Scenario: Weighted KSHORTEST under a hops limit serves only paths it can prove are the cheapest
        Given an empty graph
        And having executed:
            """
            CREATE (s:N {id: 'S'}), (b:N {id: 'B'}), (t:N {id: 'T'}),
                   (s)-[:R {w: 10}]->(t), (s)-[:R {w: 1}]->(b), (b)-[:R {w: 1}]->(t)
            """
        And having executed:
            """
            SET DATABASE SETTING 'hops_limit_partial_results' TO 'true'
            """
        When executing query:
            """
            USING HOPS LIMIT 4 MATCH (s:N {id: 'S'}), (t:N {id: 'T'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.w) total]->(t) RETURN total
            """
        Then the result should be empty

    # Four paths cost 2, 4, 6 and 10. A limit of 12 runs out after the search proves the cheapest one,
    # so the row serves that path and stops, rather than nothing or a path it has not proven.
    Scenario: Weighted KSHORTEST under a hops limit serves a proven prefix and stops
        Given an empty graph
        And having executed:
            """
            CREATE (s:N {id: 'S'}), (a:N {id: 'A'}), (b:N {id: 'B'}), (c:N {id: 'C'}), (t:N {id: 'T'}),
                   (s)-[:R {w: 1, i: 1}]->(a), (a)-[:R {w: 1, i: 2}]->(t), (s)-[:R {w: 2, i: 3}]->(b),
                   (b)-[:R {w: 2, i: 4}]->(t), (s)-[:R {w: 3, i: 5}]->(c), (c)-[:R {w: 3, i: 6}]->(t),
                   (s)-[:R {w: 10, i: 7}]->(t)
            """
        And having executed:
            """
            SET DATABASE SETTING 'hops_limit_partial_results' TO 'true'
            """
        When executing query:
            """
            USING HOPS LIMIT 12 MATCH (s:N {id: 'S'}), (t:N {id: 'T'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.w) total]->(t) RETURN [r IN relationships(p) | r.i] AS path, total
            """
        Then the result should be, in order:
            | path   | total |
            | [1, 2] | 2     |
