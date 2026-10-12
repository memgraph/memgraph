Feature: KSHORTEST weight lambda

    Scenario: Bare KSHORTEST enumerates every loopless path
        # Pre-flight: existing
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST]->(t) RETURN [x IN nodes(p) | x.id] AS path, size(relationships(p)) AS hops
            """
        Then the result should be:
            | path                 | hops |
            | ['A', 'B', 'D']      | 2    |
            | ['A', 'C', 'B', 'D'] | 3    |
            | ['A', 'D']           | 1    |
            | ['A', 'E', 'F', 'D'] | 3    |
            | ['A', 'B', 'C', 'D'] | 3    |
            | ['A', 'C', 'D']      | 2    |

    Scenario: A weight lambda with a total orders paths by total weight then hops
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.w) total]->(t) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                                                                                            |
            | [[['A', 'C', 'D'], 2], [['A', 'E', 'F', 'D'], 2], [['A', 'B', 'C', 'D'], 6], [['A', 'C', 'B', 'D'], 7], [['A', 'B', 'D'], 10], [['A', 'D'], 20]] |

    Scenario: A lone lambda is the weight lambda
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.w)]->(t) RETURN collect([x IN nodes(p) | x.id]) AS paths
            """
        Then the result should be:
            | paths                                                                                                            |
            | [['A', 'C', 'D'], ['A', 'E', 'F', 'D'], ['A', 'B', 'C', 'D'], ['A', 'C', 'B', 'D'], ['A', 'B', 'D'], ['A', 'D']] |

    Scenario: The weight lambda reads the node it reaches
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | n.cost + e.w) total]->(t) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                                                                                              |
            | [[['A', 'C', 'D'], 4], [['A', 'E', 'F', 'D'], 4], [['A', 'B', 'C', 'D'], 17], [['A', 'C', 'B', 'D'], 18], [['A', 'B', 'D'], 20], [['A', 'D'], 21]] |

    Scenario: Weight then filter, with a total
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.w) total (e, n | n.id <> 'C')]->(t)
            RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                |
            | [[['A', 'E', 'F', 'D'], 2], [['A', 'B', 'D'], 10], [['A', 'D'], 20]] |

    Scenario: A path limit keeps the cheapest paths
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST|3 (e, n | e.w) total]->(t) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                        |
            | [[['A', 'C', 'D'], 2], [['A', 'E', 'F', 'D'], 2], [['A', 'B', 'C', 'D'], 6]] |

    Scenario: An upper hop bound drops cheaper longer paths
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST ..2 (e, n | e.w) total]->(t) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                           |
            | [[['A', 'C', 'D'], 2], [['A', 'B', 'D'], 10], [['A', 'D'], 20]] |

    Scenario: A lower hop bound serves only longer paths, still by weight
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST 3.. (e, n | e.w) total]->(t) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                             |
            | [[['A', 'E', 'F', 'D'], 2], [['A', 'B', 'C', 'D'], 6], [['A', 'C', 'B', 'D'], 7]] |

    Scenario: The weight lambda reads a parameter
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        And parameters are:
            | c | 3 |
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.w * $c + 1) total]->(t)
            RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                                                                                              |
            | [[['A', 'C', 'D'], 8], [['A', 'E', 'F', 'D'], 9], [['A', 'B', 'C', 'D'], 21], [['A', 'C', 'B', 'D'], 24], [['A', 'B', 'D'], 32], [['A', 'D'], 61]] |

    Scenario: The weight lambda reads the outer row, per input row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            UNWIND [['A', 'D', 0], ['A', 'D', 100], ['C', 'D', 0]] AS row
            MATCH (s:N {id: row[0]}), (t:N {id: row[1]}) WITH s, t, row[2] AS bonus
            MATCH p=(s)-[:R *KSHORTEST (e, n | CASE n.id WHEN 'C' THEN e.w + bonus ELSE e.w END) total]->(t)
            WITH s, bonus, p, total
            RETURN s.id AS src, bonus, collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | src | bonus | paths                                                                                                                                                  |
            | 'A' | 0     | [[['A', 'C', 'D'], 2], [['A', 'E', 'F', 'D'], 2], [['A', 'B', 'C', 'D'], 6], [['A', 'C', 'B', 'D'], 7], [['A', 'B', 'D'], 10], [['A', 'D'], 20]]       |
            | 'A' | 100   | [[['A', 'E', 'F', 'D'], 2], [['A', 'B', 'D'], 10], [['A', 'D'], 20], [['A', 'C', 'D'], 102], [['A', 'B', 'C', 'D'], 106], [['A', 'C', 'B', 'D'], 107]] |
            | 'C' | 0     | [[['C', 'D'], 1], [['C', 'B', 'D'], 6]]                                                                                                                |

    Scenario: Undirected weighted KSHORTEST
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'E'}), (t:N {id: 'B'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST|4 (e, n | e.w) total]-(t) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                                                                  |
            | [[['E', 'A', 'C', 'B'], 2], [['E', 'F', 'D', 'C', 'B'], 2], [['E', 'A', 'C', 'B'], 3], [['E', 'F', 'D', 'C', 'B'], 3]] |

    Scenario: Weighted KSHORTEST inside a COUNT subquery reweighs per outer row
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            UNWIND [0, 100, 0] AS bonus
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'})
            RETURN bonus, COUNT {
              MATCH p=(s)-[:R *KSHORTEST (e, n | CASE n.id WHEN 'C' THEN e.w + bonus ELSE e.w END) total]->(t)
              WHERE total < 10
            } AS cheap
            """
        Then the result should be:
            | bonus | cheap |
            | 0     | 4     |
            | 100   | 1     |
            | 0     | 4     |

    Scenario: A constant weight with a filter is the filter-only spelling
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | 1) total (e, n | n.id <> 'C')]->(t)
            RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['A', 'B', 'D']      | 2     |
            | ['A', 'D']           | 1     |
            | ['A', 'E', 'F', 'D'] | 3     |

    Scenario: A constant weight with a limit keeps the fewest hops
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST|3 (e, n | 2) total]->(t)
            WITH collect([[x IN nodes(p) | x.id], total]) AS served
            UNWIND served AS row
            RETURN row[0] AS path, row[1] AS total, [r IN served | r[1]] AS served_totals
            """
        Then the result should be:
            | path            | total | served_totals |
            | ['A', 'B', 'D'] | 4     | [2, 4, 4]     |
            | ['A', 'C', 'D'] | 4     | [2, 4, 4]     |
            | ['A', 'D']      | 2     | [2, 4, 4]     |

    Scenario: Weighted KSHORTEST written right to left
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(t)<-[:R *KSHORTEST (e, n | n.cost + e.w) total]-(s) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                                                                                              |
            | [[['D', 'C', 'A'], 3], [['D', 'F', 'E', 'A'], 3], [['D', 'C', 'B', 'A'], 16], [['D', 'B', 'C', 'A'], 17], [['D', 'B', 'A'], 19], [['D', 'A'], 20]] |

    Scenario: Undirected weighted KSHORTEST reads the node it reaches
        # Pre-flight: new
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        When executing query:
            """
            MATCH (s:N {id: 'E'}), (t:N {id: 'B'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST|4 (e, n | n.cost + e.w) total]-(t) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                                                                      |
            | [[['E', 'A', 'C', 'B'], 12], [['E', 'A', 'C', 'B'], 13], [['E', 'F', 'D', 'C', 'B'], 13], [['E', 'F', 'D', 'C', 'B'], 14]] |

    # Every 3-hop path weighs 0.3 + 0.1 + 0.5. Added left to right that is 0.9; added from the end it is
    # 0.8999999999999999. Each path's total must be the left-to-right sum, whichever deviation found it.
    Scenario: Weighted KSHORTEST totals are the same sum for the same weights
        Given an empty graph
        And having executed:
            """
            CREATE (a:N {id: 0}), (b:N {id: 1}), (c:N {id: 2}), (t:N {id: 3}),
                   (a)-[:R {w: 0.3}]->(b), (a)-[:R {w: 0.3}]->(b), (b)-[:R {w: 0.1}]->(c), (b)-[:R {w: 0.1}]->(c),
                   (c)-[:R {w: 0.5}]->(t), (c)-[:R {w: 0.5}]->(t), (a)-[:R {w: 0.9}]->(t)
            """
        When executing query:
            """
            MATCH (a:N {id: 0}), (t:N {id: 3}) WITH a, t
            MATCH p=(a)-[:R *KSHORTEST (e, n | e.w) total]->(t) RETURN collect(total) AS totals
            """
        Then the result should be:
            | totals                                            |
            | [0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9]     |

    # Each graph's served path is cheap, but the search also adds weights near 2^62 that no served path
    # carries: in the reverse tree (1), in a spur's priority (2) and in a spur's cost (3).
    Scenario: Weighted KSHORTEST overflows only on a served total
        Given an empty graph
        And having executed:
            """
            CREATE (s1:N {id: 's1'})-[:R {w: 1}]->(t1:N {id: 't1'}),
                   (:N {id: 'y1'})-[:R {w: 4611686018427387904}]->(:N {id: 'x1'})-[:R {w: 4611686018427387904}]->(t1),
                   (s2:N {id: 's2'})-[:R {w: 1}]->(t2:N {id: 't2'}),
                   (s2)-[:R {w: 4611686018427387904}]->(:N {id: 'x2'})-[:R {w: 4611686018427387904}]->(t2),
                   (s3:N {id: 's3'})-[:R {w: 1}]->(a3:N {id: 'a3'})-[:R {w: 1}]->(t3:N {id: 't3'}),
                   (a3)-[:R {w: 9223372036854775807}]->(:N {id: 'c3'})-[:R {w: 0}]->(t3)
            """
        When executing query:
            """
            UNWIND ['1', '2', '3'] AS g
            MATCH (s:N {id: 's' + g}), (t:N {id: 't' + g}) WITH g, s, t
            MATCH p=(s)-[:R *KSHORTEST|1 (e, n | e.w) total]->(t) RETURN g, total ORDER BY g
            """
        Then the result should be, in order:
            | g   | total |
            | '1' | 1     |
            | '2' | 1     |
            | '3' | 2     |
