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

    Scenario: Advance command on subquery should not affect outer query vertex visibility
        Given an empty graph
        And having executed
            """
            CREATE (a:User {id: "user1", name: "Alice"})
            CREATE (b:Post {id: "post1", title: "Hello"})
            CREATE (c:Post {id: "post2", title: "World"})
            CREATE (a)-[:HAS_POST]->(b)
            CREATE (a)-[:HAS_POST]->(c)
            """
        When executing query:
            """
            MATCH (user:User {id: "user1"})
            CALL {
                WITH user
                OPTIONAL MATCH (user)-[rel:HAS_POST]->(post:Post)
                WITH rel, collect(DISTINCT post) AS posts
                CALL {
                    WITH posts
                    UNWIND posts AS post
                    DETACH DELETE post
                }
            }
            RETURN user.name AS name, user.id AS id
            """
        Then the result should be:
            | name    | id      |
            | 'Alice' | 'user1' |

    Scenario: Match after call
        Given an empty graph
        And having executed
            """
            CREATE ({n0:0})
            """
        When executing query:
            """
            CALL {
            	RETURN 0 AS x
            }
            MATCH (n {n0:x})
            RETURN n.n0 as n0;
            """
        Then the result should be:
            | n0 |
            | 0  |

    Scenario: Unwind in subquery passes correctly with same named symbol
        Given an empty graph
        When executing query:
            """
            CREATE (this0 {id: 1})
            WITH this0
            CALL {
                WITH this0
                WITH collect(this0) as parentNodes
                CALL {
                    WITH parentNodes
                    UNWIND parentNodes as this0
                    create ()
                }
            }
            RETURN this0.id AS id
            """
        Then the result should be:
            | id |
            | 1  |

    Scenario: Subquery with union missing WITH in second branch should fail
        Given an empty graph
        When executing query:
            """
            MATCH (n:Node {flagged: 1})
            CALL {
                WITH n
                MATCH (m:Node) WHERE m.prop_a = n.prop_a RETURN m
                UNION
                MATCH (m:Node) WHERE m.prop_b = n.prop_b RETURN m
            }
            RETURN m.prop_a
            """
        Then an error should be raised

    Scenario: Scoped CALL importing no variables returns constant from each input row
        Given graph "subqueries"
        When executing query:
            """
            UNWIND [0, 1, 2] AS x
            CALL () {
              RETURN 'hello' AS innerReturn
            }
            RETURN innerReturn
            """
        Then the result should be:
            | innerReturn |
            | 'hello'     |
            | 'hello'     |
            | 'hello'     |

    Scenario: Scoped CALL importing no variables performs incremental updates
        Given graph "subqueries"
        When executing query:
            """
            UNWIND [1, 2, 3] AS x
            CALL () {
                MATCH (p:Player {name: 'Player A'})
                SET p.age = p.age + 1
                RETURN p.age AS newAge
            }
            MATCH (p:Player {name: 'Player A'})
            RETURN x AS iteration, newAge, p.age AS totalAge
            """
        Then the result should be:
            | iteration | newAge | totalAge |
            | 1         | 22     | 24       |
            | 2         | 23     | 24       |
            | 3         | 24     | 24       |

    Scenario: Scoped CALL imports only the named variable
        Given graph "subqueries"
        When executing query:
            """
            MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
            CALL (p) {
              WITH p.age / 100.0 AS random
              SET p.rating = random
              RETURN p.name AS playerName, p.rating AS rating
            }
            RETURN playerName, rating, t AS team
            ORDER BY rating, t.name
            LIMIT 1
            """
        Then the result should be:
            | playerName | rating | team                     |
            | 'Player A' | 0.21   | (:Team {name: 'Team A'}) |

    Scenario: Scoped CALL with star imports every outer variable
        Given graph "subqueries"
        When executing query:
            """
            MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
            CALL (*) {
              SET p.lastUpdated = 1719304206653
              SET t.lastUpdated = 1719304206653
            }
            RETURN p.name AS playerName,
                   p.lastUpdated AS playerUpdated,
                   t.name AS teamName,
                   t.lastUpdated AS teamUpdated
            ORDER BY p.name, t.name
            LIMIT 1
            """
        Then the result should be:
            | playerName | playerUpdated | teamName | teamUpdated   |
            | 'Player A' | 1719304206653 | 'Team A' | 1719304206653 |

    Scenario: Scoped CALL with empty imports exposes subquery cardinality to outer
        Given graph "subqueries"
        When executing query:
            """
            MATCH (t:Team)
            CALL () {
              MATCH (p:Player)
              RETURN count(p) AS totalPlayers
            }
            RETURN count(t) AS totalTeams, totalPlayers
            """
        Then the result should be:
            | totalTeams | totalPlayers |
            | 3          | 6            |

    Scenario: OPTIONAL CALL scoped subquery is not supported
        Given graph "subqueries"
        When executing query:
            """
            MATCH (p:Player)
            OPTIONAL CALL (p) {
                MATCH (p)-[:PLAYS_FOR]->(team:Team)
                RETURN team.name AS team
            }
            RETURN p.name AS playerName, team
            """
        Then an error should be raised

    Scenario: Scoped CALL subquery with UNION over two ORDER BY branches
        Given graph "subqueries"
        When executing query:
            """
            CALL () {
              MATCH (p:Player)
              RETURN p
              ORDER BY p.age ASC
              LIMIT 1
            UNION
              MATCH (p:Player)
              RETURN p
              ORDER BY p.age DESC
              LIMIT 1
            }
            RETURN p.name AS playerName, p.age AS age
            """
        Then the result should be:
            | playerName | age |
            | 'Player C' | 19  |
            | 'Player F' | 35  |

    Scenario: Scoped CALL with imported variable used in both UNION ALL branches
        Given graph "subqueries"
        When executing query:
            """
            MATCH (t:Team)
            CALL (t) {
              OPTIONAL MATCH (t)-[o:OWES]->(other:Team)
              RETURN o.dollars * -1 AS moneyOwed
              UNION ALL
              OPTIONAL MATCH (other)-[o:OWES]->(t)
              RETURN o.dollars AS moneyOwed
            }
            RETURN t.name AS team, sum(moneyOwed) AS amountOwed
            ORDER BY amountOwed DESC
            """
        Then the result should be:
            | team     | amountOwed |
            | 'Team B' | 7800       |
            | 'Team C' | -3300      |
            | 'Team A' | -4500      |

    Scenario: Returning scoped CALL subquery trims outer rows with no match
        Given graph "subqueries"
        When executing query:
            """
            MATCH (p:Player)
            CALL (p) {
              MATCH (p)-[:PLAYS_FOR]->(team:Team)
              RETURN team.name AS team
            }
            RETURN p.name AS playerName, team
            """
        Then the result should be:
            | playerName | team     |
            | 'Player A' | 'Team A' |
            | 'Player B' | 'Team A' |
            | 'Player D' | 'Team B' |
            | 'Player E' | 'Team C' |
            | 'Player F' | 'Team C' |

    Scenario: Unit scoped CALL subquery preserves outer row count
        Given graph "subqueries"
        When executing query:
            """
            MATCH (p:Player)
            CALL (p) {
              UNWIND range(1, 3) AS i
              CREATE (:Person {name: p.name})
            }
            RETURN count(*)
            """
        Then the result should be:
            | count(*) |
            | 6        |

    Scenario: Aggregation over scoped CALL with imported variable
        Given graph "subqueries"
        When executing query:
            """
            MATCH (t:Team)
            CALL (t) {
              MATCH (t)-[o:OWES]->(t2:Team)
              RETURN sum(o.dollars) AS owedAmount, t2.name AS owedTeam
            }
            RETURN t.name AS owingTeam, owedAmount, owedTeam
            """
        Then the result should be:
            | owingTeam | owedAmount | owedTeam |
            | 'Team A'  | 4500       | 'Team B' |
            | 'Team B'  | 1700       | 'Team C' |
            | 'Team C'  | 5000       | 'Team B' |

    Scenario: Scoped CALL with collect builds per-group list (performance pattern)
        Given graph "subqueries"
        When executing query:
            """
            MATCH (t:Team)
            CALL (t) {
              MATCH (p:Player)-[:PLAYS_FOR]->(t)
              RETURN collect(p) AS players
            }
            RETURN t AS team, players
            """
        Then the result should be:
            | team                      | players                                                                        |
            | (:Team {name: 'Team A'})  | [(:Player {name: 'Player A', age: 21}), (:Player {name: 'Player B', age: 23})] |
            | (:Team {name: 'Team B'})  | [(:Player {name: 'Player D', age: 30})]                                        |
            | (:Team {name: 'Team C'})  | [(:Player {name: 'Player E', age: 25}), (:Player {name: 'Player F', age: 35})] |

    Scenario: Aliasing variables in the scope clause is not allowed
        Given graph "subqueries"
        When executing query:
            """
            MATCH (t:Team)
            CALL (t AS teams) {
            MATCH (p:Player)-[:PLAYS_FOR]->(teams)
            RETURN collect(p) AS players
            }
            RETURN t AS teams, players
            """
        Then an error should be raised

    Scenario: Re-declaring a scoped import inside the subquery is not allowed
        Given graph "subqueries"
        When executing query:
            """
            MATCH (t:Team)
            CALL (t) {
              WITH 'New team' AS t
              MATCH (p:Player)-[:PLAYS_FOR]->(t)
              RETURN collect(p) AS players
            }
            RETURN t AS team, players
            """
        Then an error should be raised

    Scenario: Subquery must not return a name that already exists in outer scope
        Given graph "subqueries"
        When executing query:
            """
            MATCH (t:Team)
            CALL () {
              RETURN 1 AS t
            }
            RETURN t
            """
        Then an error should be raised

    Scenario: Scoped CALL with unknown imported variable raises unbound error
        Given graph "subqueries"
        When executing query:
            """
            CALL (doesNotExist) {
              RETURN 1 AS x
            }
            RETURN x
            """
        Then an error should be raised

    Scenario: Scoped CALL with duplicate imports raises syntax error
        Given graph "subqueries"
        When executing query:
            """
            MATCH (p:Player)
            CALL (p, p) {
              RETURN 1 AS x
            }
            RETURN x
            """
        Then an error should be raised

    Scenario: Scoped CALL with empty imports cannot see outer variables
        Given graph "subqueries"
        When executing query:
            """
            MATCH (p:Player)
            CALL () {
              RETURN p.name AS playerName
            }
            RETURN playerName
            """
        Then an error should be raised
