Feature: Weighted Shortest Path

  Scenario: Test match wShortest upper bound
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: 1}]->({a:'1'})-[:r {w: 1}]->({a:'2'}), (n)-[:r {w: 1}]->({a:'3'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[le *wShortest 1 (e, n | e.w ) w]->(m) RETURN m.a
          """
      Then the result should be:
          | m.a |
          | '1' |
          | '3' |

  Scenario: Test match wShortest filtered
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: 1}]->({a:'1'})-[:r {w: 1}]->({a:'2'}), (n)-[:r {w: 1}]->({a:'3'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[le *wShortest 1 (e, n | e.w ) w (e, n | n.a = '3')]->(m) RETURN m.a
          """
      Then the result should be:
          | m.a |
          | '3' |

  Scenario: Test match wShortest resulting edge list
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: 1}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 4}]->({a:'3'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[le *wShortest 10 (e, n | e.w ) w]->(m) RETURN m.a, size(le) as s, w
          """
      Then the result should be:
          | m.a | s | w |
          | '1' | 1 | 1 |
          | '2' | 2 | 3 |
          | '3' | 1 | 4 |

  Scenario: Test match wShortest single edge type filtered
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r0 {w: 1}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
          """
      When executing query:
          """
          MATCH ()-[le:r0 *wShortest 10 (e, n | e.w) w]->(m)
          RETURN size(le) AS s, m.a
          """
      Then the result should be:
          | s | m.a  |
          | 1 | '1'  |

  Scenario: Test match wShortest multiple edge types filtered
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r0 {w: 1}]->({a:'1'})-[:r1 {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
          """
      When executing query:
          """
          MATCH ()-[le :r0|:r1 *wShortest 10 (e, n | e.w) w]->(m) WHERE size(le) > 1
          RETURN size(le) AS s, (le[0]).w AS r0, (le[1]).w AS r1
          """
      Then the result should be:
          | s | r0 | r1 |
          | 2 | 1  | 2  |

  Scenario: Test match wShortest property filters
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: 1}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
          """
      When executing query:
          """
          MATCH ()-[le *wShortest 10 {w:1} (e, n | e.w ) total_weight]->(m)
          RETURN size(le) AS s, (le[0]).w AS r0
          """
      Then the result should be:
          | s | r0 |
          | 1 | 1  |

  Scenario: Test match wShortest weight not a number
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: 'not a number'}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
          """
      When executing query:
          """
          MATCH ()-[le *wShortest 10 (e, n | e.w ) total_weight]->(m)
          RETURN le, total_weight
          """
      Then an error should be raised

  Scenario: Test match wShortest negative weight
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: -1}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
          """
      When executing query:
          """
          MATCH ()-[le *wShortest 10 (e, n | e.w ) total_weight]->(m)
          RETURN le, total_weight
          """
      Then an error should be raised

  Scenario: Test match wShortest weight duration
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: DURATION('PT1S')}]->({a:'1'})-[:r {w: DURATION('PT2S')}]->({a:'2'}), (n)-[:r {w: DURATION('PT4S')}]->({a:'3'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[le *wShortest 10 (e, n | e.w ) w]->(m) RETURN m.a, size(le) as s, w
          """
      Then the result should be:
          | m.a | s | w    |
          | '1' | 1 | PT1S |
          | '2' | 2 | PT3S |
          | '3' | 1 | PT4S |

  Scenario: Test match wShortest weight negative duration
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: DURATION({seconds: -1})}]->({a:'1'})-[:r {w: DURATION('PT2S')}]->({a:'2'}), (n)-[:r {w: DURATION('PT4S')}]->({a:'3'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[le *wShortest 10 (e, n | e.w ) w]->(m) RETURN m.a, size(le) as s, w
          """
      Then an error should be raised

  Scenario: Test match wShortest weight mixed numeric and duration as weights
      Given an empty graph
      And having executed:
          """
          CREATE (n {a:'0'})-[:r {w: 2}]->({a:'1'})-[:r {w: DURATION('PT2S')}]->({a:'2'}), (n)-[:r {w: DURATION('PT4S')}]->({a:'3'})
          """
      When executing query:
          """
          MATCH (n {a:'0'})-[le *wShortest 10 (e, n | e.w ) w]->(m) RETURN m.a, size(le) as s, w
          """
      Then an error should be raised
