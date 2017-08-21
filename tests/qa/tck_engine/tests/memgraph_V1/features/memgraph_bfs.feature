Feature: Bfs

  Scenario: Test match BFS depth blocked
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r]->({a:'1.1'})-[:r]->({a:'2.1'}), (n)-[:r]->({a:'1.2'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-bfs(e, m| true, 1)->(m) RETURN n.a, m.a
          """
      Then the result should be:
          | n.a | m.a   |
          | '0' | '1.1' |
          | '0' | '1.2' |

  Scenario: Test match BFS filtered
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r]->({a:'1.1'})-[:r]->({a:'2.1'}), (n)-[:r]->({a:'1.2'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-bfs(e, m| m.a = '1.1' OR m.a = '0', 10)->(m) RETURN n.a, m.a
          """
      Then the result should be:
          | n.a | m.a   |
          | '0' | '1.1' |
