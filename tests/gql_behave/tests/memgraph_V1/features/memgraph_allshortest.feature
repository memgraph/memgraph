Feature: All Shortest Path

    Scenario: Test match allShortest upper bound
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: 1}]->({a:'1'})-[:r {w: 1}]->({a:'2'}), (n)-[:r {w: 1}]->({a:'3'})
            """
        When executing query:
            """
            MATCH (n {a:'0'})-[le *allShortest 1 (e, n | e.w ) w]->(m) RETURN m.a
            """
        Then the result should be:
            | m.a |
            | '1' |
            | '3' |

    Scenario: Test match allShortest upper bound 2
        Given an empty graph
        And having executed:
            """
            CREATE (a {a:'0'})-[:r {w: 2}]->(b {a:'1'})-[:r {w: 3}]->(c {a:'2'}),
                (a)-[:re {w: 2}]->(b),
                (b)-[:re {w:3}]->(c),
                ({a: '4'})<-[:r {w: 1}]-(a),
                ({a: '5'})<-[:r {w: 1}]-(a),
                (c)-[:r {w: 1}]->({a: '6'}),
                (c)-[:r {w: 1}]->({a: '7'})
            """
        When executing query:
            """
            MATCH path=(n {a:'0'})-[r *allShortest ..2 (e, n | 1 ) w]->(m {a:'2'}) RETURN COUNT(path) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: Test match allShortest filtered
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: 1}]->({a:'1'})-[:r {w: 1}]->({a:'2'}), (n)-[:r {w: 1}]->({a:'3'})
            """
        When executing query:
            """
            MATCH (n {a:'0'})-[le *allShortest 1 (e, n | e.w ) w (e, n | n.a = '3')]->(m) RETURN m.a
            """
        Then the result should be:
            | m.a |
            | '3' |

    Scenario: Test match allShortest resulting edge list
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: 1}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 4}]->({a:'3'})
            """
        When executing query:
            """
            MATCH (n {a:'0'})-[le *allShortest 10 (e, n | e.w ) w]->(m) RETURN m.a, size(le) as s, w
            """
        Then the result should be:
            | m.a | s | w |
            | '1' | 1 | 1 |
            | '2' | 2 | 3 |
            | '3' | 1 | 4 |

    Scenario: Test match allShortest single edge type filtered
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r0 {w: 1}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
            """
        When executing query:
            """
            MATCH ()-[le:r0 *allShortest 10 (e, n | e.w) w]->(m)
            RETURN size(le) AS s, m.a
            """
        Then the result should be:
            | s | m.a  |
            | 1 | '1'  |

    Scenario: Test match allShortest multiple edge types filtered
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r0 {w: 1}]->({a:'1'})-[:r1 {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
            """
        When executing query:
            """
            MATCH ()-[le :r0|:r1 *allShortest 10 (e, n | e.w) w]->(m) WHERE size(le) > 1
            RETURN size(le) AS s, (le[0]).w AS r0, (le[1]).w AS r1
            """
        Then the result should be:
            | s | r0 | r1 |
            | 2 | 1  | 2  |

    Scenario: Test match allShortest property filters
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: 1}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
            """
        When executing query:
            """
            MATCH ()-[le *allShortest 10 {w:1} (e, n | e.w ) total_weight]->(m)
            RETURN size(le) AS s, (le[0]).w AS r0
            """
        Then the result should be:
            | s | r0 |
            | 1 | 1  |

    Scenario: Test match allShortest weight not a number
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: 'not a number'}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
            """
        When executing query:
            """
            MATCH ()-[le *allShortest 10 (e, n | e.w ) total_weight]->(m)
            RETURN le, total_weight
            """
        Then an error should be raised

    Scenario: Test match allShortest negative weight
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: -1}]->({a:'1'})-[:r {w: 2}]->({a:'2'}), (n)-[:r {w: 3}]->({a:'4'})
            """
        When executing query:
            """
            MATCH ()-[le *allShortest 10 (e, n | e.w ) total_weight]->(m)
            RETURN le, total_weight
            """
        Then an error should be raised

    Scenario: Test match allShortest weight duration
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: DURATION('PT1S')}]->({a:'1'})-[:r {w: DURATION('PT2S')}]->({a:'2'}), (n)-[:r {w: DURATION('PT4S')}]->({a:'3'})
            """
        When executing query:
            """
            MATCH (n {a:'0'})-[le *allShortest 10 (e, n | e.w ) w]->(m) RETURN m.a, size(le) as s, w
            """
        Then the result should be:
            | m.a | s | w    |
            | '1' | 1 | PT1S |
            | '2' | 2 | PT3S |
            | '3' | 1 | PT4S |

    Scenario: Test match allShortest weight negative duration
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: DURATION({seconds: -1})}]->({a:'1'})-[:r {w: DURATION('PT2S')}]->({a:'2'}), (n)-[:r {w: DURATION('PT4S')}]->({a:'3'})
            """
        When executing query:
            """
            MATCH (n {a:'0'})-[le *allShortest 10 (e, n | e.w ) w]->(m) RETURN m.a, size(le) as s, w
            """
        Then an error should be raised

    Scenario: Test match allShortest weight mixed numeric and duration as weights
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: 2}]->({a:'1'})-[:r {w: DURATION('PT2S')}]->({a:'2'}), (n)-[:r {w: DURATION('PT4S')}]->({a:'3'})
            """
        When executing query:
            """
            MATCH (n {a:'0'})-[le *allShortest 10 (e, n | e.w ) w]->(m) RETURN m.a, size(le) as s, w
            """
        Then an error should be raised

    Scenario: Test allShortest return both paths of same length
        Given an empty graph
        And having executed:
            """
            CREATE (n {a:'0'})-[:r {w: 2}]->({a:'1'})-[:r {w: 3}]->({a:'2'}), (n)-[:r {w: 5}]->({a:'2'})
            """
        When executing query:
            """
            MATCH path=(n {a:'0'})-[r *allShortest (e, n | e.w ) w]->(m {a:'2'}) RETURN COUNT(path);
            """
        Then the result should be:
            | COUNT(path) |
            | 2 |

    Scenario: Test allShortest on different edge between two nodes
        Given an empty graph
        And having executed:
            """
            CREATE (n:One), (o:Two), (m:Three), (n)-[:TYPE {cost: 0.3}]->(o), (o)-[:TYPE {cost: 40}]->(m), (o)-[:TYPE {cost: 20}]->(m)
            """
        When executing query:
            """
            MATCH p=(h:One)-[r*allshortest ..5 (e, v | e.cost) total_cost]->(k:Three) return total_cost;
            """
        Then the result should be:
            | total_cost |
            | 20.3       |

    Scenario: Test match allShortest with accumulated path filtered by order of ids
      Given an empty graph
      And having executed:
          """
          CREATE (:label1 {id: 1})-[:type1 {id:1}]->(:label2 {id: 2})-[:type1 {id: 2}]->(:label3 {id: 3})-[:type1 {id: 3}]->(:label4 {id: 4});
          """
      When executing query:
          """
          MATCH pth=(:label1)-[*ALLSHORTEST (r, n | r.id) total_weight (e,n,p | e.id > 0 and (nodes(p)[-1]).id > (nodes(p)[-2]).id)]->(:label4) RETURN pth, total_weight;
          """
      Then the result should be:
          | pth                                                                                                               | total_weight   |
          | <(:label1{id:1})-[:type1{id:1}]->(:label2{id:2})-[:type1{id:2}]->(:label3{id:3})-[:type1{id:3}]->(:label4{id:4})> | 6              |

    Scenario: Test match allShortest with accumulated path filtered by edge type1
      Given graph "graph_edges"
      When executing query:
          """
          MATCH path=(:label1)-[*ALLSHORTEST (r, n | r.id) total_weight (e, n, p | NOT(type(e)='type1' AND type(last(relationships(p))) = 'type1'))]->(:label3) RETURN path, total_weight;
          """
      Then the result should be:
          | path                                                     | total_weight   |
          | <(:label1 {id: 1})-[:type2 {id: 10}]->(:label3 {id: 3})> | 10             |

    Scenario: Test match allShortest with accumulated path filtered by edge type2
      Given graph "graph_edges"
      When executing query:
          """
          MATCH path=(:label1)-[*ALLSHORTEST (r, n | r.id) total_weight (e, n, p | NOT(type(e)='type2' AND type(last(relationships(p))) = 'type2'))]->(:label3) RETURN path, total_weight;
          """
      Then the result should be:
          | path                                                                                        | total_weight   |
          | <(:label1 {id: 1})-[:type1 {id: 1}]->(:label2 {id: 2})-[:type1 {id: 2}]->(:label3 {id: 3})> | 3              |
