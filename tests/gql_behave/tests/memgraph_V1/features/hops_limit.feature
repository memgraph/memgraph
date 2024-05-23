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

    Scenario: Test hops limit DFS test01
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *]->(e) RETURN p
            """
        Then the result should be:
            | p                                                                                                      |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})>                                                |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})>                                                |
            | <(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})>                                                |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})>              |
            | <(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'E'})>                                                |


    Scenario: Test hops limit DFS test02
        Given graph "simple_binary_tree"
        And having executed:
            """
            CREATE INDEX ON :Node(name)
            """
        And having executed:
            """
            CREATE (a:Node {name: 'A'}),
            (b:Node {name: 'B'}),
            (c:Node {name: 'C'}),
            (d:Node {name: 'D'}),
            (e:Node {name: 'E'}),
            (f:Node {name: 'F'}),
            (g:Node {name: 'G'}),
            (h:Node {name: 'H'}),
            (i:Node {name: 'I'}),
            (j:Node {name: 'J'}),
            (k:Node {name: 'K'}),
            (l:Node {name: 'L'})

            CREATE (a)-[:CONNECTED]->(b),
            (a)-[:CONNECTED]->(c),
            (b)-[:CONNECTED]->(d),
            (b)-[:CONNECTED]->(e),
            (c)-[:CONNECTED]->(f),
            (c)-[:CONNECTED]->(g),
            (d)-[:CONNECTED]->(h),
            (d)-[:CONNECTED]->(i),
            (e)-[:CONNECTED]->(j),
            (e)-[:CONNECTED]->(k),
            (f)-[:CONNECTED]->(l);
            """
        When executing query:
            """
            USING HOPS LIMIT 2 MATCH p=(a:Node {name: 'A'})-[:CONNECTED *]->(d:Node {name: 'D'}) return p
            """
        Then the result should be:
            | p                                                                                            |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})>    |


    Scenario: Test hops limit DFS test03
        Given graph "simple_binary_tree"
        And having executed:
            """
            CREATE INDEX ON :Node(name)
            """

        When executing query:
            """
            USING HOPS LIMIT 1 MATCH p=(a:Node {name: 'A'})-[:CONNECTED *]->(d:Node {name: 'D'}) return p
            """
        Then the result should be empty



    Scenario: Test hops limit DFS both directions
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *]-(e) RETURN p
            """
        Then the result should be:
            | p                                                                                         |
            | <(:Node {name: 'B'})<-[:CONNECTED]-(:Node {name: 'A'})>                                   |
            | <(:Node {name: 'D'})<-[:CONNECTED]-(:Node {name: 'B'})<-[:CONNECTED]-(:Node {name: 'A'})> |
            | <(:Node {name: 'E'})<-[:CONNECTED]-(:Node {name: 'B'})<-[:CONNECTED]-(:Node {name: 'A'})> |
            | <(:Node {name: 'C'})<-[:CONNECTED]-(:Node {name: 'A'})>                                   |

    Scenario: Test hops limit BFS test01
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


    Scenario: Test hops limit BFS test02
        Given graph "simple_binary_tree"
        And having executed:
            """
            CREATE INDEX ON :Node(name)
            """
        And having executed:
            """
            CREATE (a:Node {name: 'A'}),
            (b:Node {name: 'B'}),
            (c:Node {name: 'C'}),
            (d:Node {name: 'D'}),
            (e:Node {name: 'E'}),
            (f:Node {name: 'F'}),
            (g:Node {name: 'G'}),
            (h:Node {name: 'H'}),
            (i:Node {name: 'I'}),
            (j:Node {name: 'J'}),
            (k:Node {name: 'K'}),
            (l:Node {name: 'L'})

            CREATE (a)-[:CONNECTED]->(b),
            (a)-[:CONNECTED]->(c),
            (b)-[:CONNECTED]->(d),
            (b)-[:CONNECTED]->(e),
            (c)-[:CONNECTED]->(f),
            (c)-[:CONNECTED]->(g),
            (d)-[:CONNECTED]->(h),
            (d)-[:CONNECTED]->(i),
            (e)-[:CONNECTED]->(j),
            (e)-[:CONNECTED]->(k),
            (f)-[:CONNECTED]->(l);
            """
        When executing query:
            """
            USING HOPS LIMIT 3 MATCH p=(a:Node {name: 'A'})-[:CONNECTED *BFS]->(d:Node {name: 'D'}) return p
            """
        Then the result should be:
            | p                                                                                                      |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})-[:CONNECTED]->(:Node {name: 'D'})>              |


    Scenario: Test hops limit BFS test03
        Given graph "simple_binary_tree"
        And having executed:
            """
            CREATE INDEX ON :Node(name)
            """

        When executing query:
            """
            USING HOPS LIMIT 2 MATCH p=(a:Node {name: 'A'})-[:CONNECTED *BFS]->(d:Node {name: 'D'}) return p
            """
        Then the result should be empty


    Scenario: Test hops limit BFS both directions
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *BFS]-(e) RETURN p
            """
        Then the result should be:
            | p                                                                                         |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})>                                   |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})>                                   |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})-[:CONNECTED]->(:Node {name: 'G'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'C'})-[:CONNECTED]->(:Node {name: 'F'})> |


    Scenario: Test simple expand
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


    Scenario: Test simple expand both directions
        Given graph "simple_binary_tree"
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED]-(b) RETURN p
            """
        Then the result should be:
            | p                                                       |
            | <(:Node {name: 'B'})<-[:CONNECTED]-(:Node {name: 'A'})> |
            | <(:Node {name: 'C'})<-[:CONNECTED]-(:Node {name: 'A'})> |
            | <(:Node {name: 'A'})-[:CONNECTED]->(:Node {name: 'B'})> |
            | <(:Node {name: 'D'})<-[:CONNECTED]-(:Node {name: 'B'})> |
            | <(:Node {name: 'E'})<-[:CONNECTED]-(:Node {name: 'B'})> |


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
            CREATE (a)-[:CONNECTED]->(b)
            CREATE (a)-[:CONNECTED]->(c)
            CREATE (a)-[:CONNECTED]->(d)
            CREATE (a)-[:CONNECTED]->(e)
            CREATE (a)-[:CONNECTED]->(f)
            """
        When executing query:
            """
            USING HOPS LIMIT 5 MATCH p=(a)-[:CONNECTED *]->(e) RETURN p
            """
        Then the result should be:
            | p                     |
            | <()-[:CONNECTED]->()> |
            | <()-[:CONNECTED]->()> |
            | <()-[:CONNECTED]->()> |
            | <()-[:CONNECTED]->()> |
            | <()-[:CONNECTED]->()> |
