Feature: Bfs

  Scenario: Test match BFS upper bound
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r]->({a:'1.1'})-[:r]->({a:'2.1'}), (n)-[:r]->({a:'1.2'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[*bfs..1]->(m) RETURN n.a, m.a
          """
      Then the result should be:
          | n.a | m.a   |
          | '0' | '1.1' |
          | '0' | '1.2' |

  Scenario: Test match BFS lower bound
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r]->({a:'1.1'})-[:r]->({a:'2.1'})-[:r]->({a:'3.1'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[*bfs 2..]->(m) RETURN n.a, m.a
          """
      Then the result should be:
          | n.a | m.a   |
          | '0' | '2.1' |
          | '0' | '3.1' |

  Scenario: Test match BFS filtered
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r]->({a:'1.1'})-[:r]->({a:'2.1'}), (n)-[:r]->({a:'1.2'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[*bfs..10 (e, m| m.a = '1.1' OR m.a = '0')]->(m) RETURN n.a, m.a
          """
      Then the result should be:
          | n.a | m.a   |
          | '0' | '1.1' |

  Scenario: Test match BFS resulting edge list
      Given an empty graph
      And having executed:
          """
          CREATE (:Starting)-[:r {id: 0}]->()-[:r {id: 1}]->()-[:r {id: 2}]->()-[:r {id: 3}]->()
          """
      When executing query:
          """
          MATCH (:Starting)-[r *bfs..10]->(m) WHERE size(r) > 3
          RETURN size(r) AS s, (r[0]).id AS r0, (r[2]).id AS r2
          """
      Then the result should be:
          | s | r0 | r2 |
          | 4 | 0  | 2  |

  Scenario: Test match BFS single edge type filtered
      Given an empty graph
      And having executed:
          """
          CREATE ()-[:r0 {id: 0}]->()-[:r1 {id: 1}]->()-[:r2 {id: 2}]->()-[:r3 {id: 3}]->()
          """
      When executing query:
          """
          MATCH ()-[r:r0 *bfs..10]->(m)
          RETURN size(r) AS s, (r[0]).id AS r0
          """
      Then the result should be:
          | s | r0 |
          | 1 | 0  |

  Scenario: Test match BFS multiple edge types filtered
      Given an empty graph
      And having executed:
          """
          CREATE ()-[:r0 {id: 0}]->()-[:r1 {id: 1}]->()-[:r2 {id: 2}]->()-[:r3 {id: 3}]->()
          """
      When executing query:
          """
          MATCH ()-[r :r0|:r1 *bfs..10 ]->(m) WHERE size(r) > 1
          RETURN size(r) AS s, (r[0]).id AS r0, (r[1]).id AS r1
          """
      Then the result should be:
          | s | r0 | r1 |
          | 2 | 0  | 1  |

  Scenario: Test match BFS property filters
      Given an empty graph
      And having executed:
          """
          CREATE ()-[:r0 {id: 0}]->()-[:r1 {id: 1}]->()-[:r2 {id: 2}]->()-[:r3 {id: 3}]->()
          """
      When executing query:
          """
          MATCH ()-[r *bfs..10 {id: 1}]->(m)
          RETURN size(r) AS s, (r[0]).id AS r0
          """
      Then the result should be:
          | s | r0 |
          | 1 | 1  |

  Scenario: Test accessing a variable bound to a list within BFS function
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 0})-[:LINK {date: '2023-03'}]->(:Node {id: 1}),
          (:Node {id: 1})-[:LINK {date: '2023-04'}]->(:Node {id: 2}),
          (:Node {id: 2})-[:LINK {date: '2023-03'}]->(:Node {id: 3});
          """
      When executing query:
          """
          WITH ['2023-03'] AS date
          MATCH p = (:Node)-[*BFS ..2 (r, n | r.date IN date)]->(:Node {id: 3})
          RETURN p
          """
      Then the result should be:
         | p                                                              |
         | <(:Node {id: 2})-[:LINK {date: '2023-03'}]->(:Node {id: 3})>   |
