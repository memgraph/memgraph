Feature: KSHORTEST weight lambda

    Scenario: Bare KSHORTEST enumerates every loopless path by hop count
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

    Scenario: Bare KSHORTEST enumerates every loopless path by hop count (indexes :N(id))
        # Pre-flight: existing
        Given an empty graph
        And with new index :N(id)
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
            | ['A', 'E', 'F', 'D'] | 3    |
            | ['A', 'B', 'C', 'D'] | 3    |
            | ['A', 'C', 'D']      | 2    |
            | ['A', 'D']           | 1    |
            | ['A', 'B', 'D']      | 2    |
            | ['A', 'C', 'B', 'D'] | 3    |

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

    Scenario: A weight lambda with a total orders paths by total weight then hops (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: A lone lambda is the weight lambda (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: The weight lambda reads the node it reaches (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: Weight then filter, with a total (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: Weight then filter, without a total
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.w) (e, n | n.id <> 'C')]->(t)
            RETURN collect([x IN nodes(p) | x.id]) AS paths
            """
        Then the result should be:
            | paths                                               |
            | [['A', 'E', 'F', 'D'], ['A', 'B', 'D'], ['A', 'D']] |

    Scenario: Weight then filter, without a total (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.w) (e, n | n.id <> 'C')]->(t)
            RETURN collect([x IN nodes(p) | x.id]) AS paths
            """
        Then the result should be:
            | paths                                               |
            | [['A', 'E', 'F', 'D'], ['A', 'B', 'D'], ['A', 'D']] |

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

    Scenario: A path limit keeps the cheapest paths (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: An upper hop bound drops cheaper longer paths (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: A lower hop bound serves only longer paths, still by weight (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: The weight lambda reads a parameter (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: The weight lambda reads the outer row, per input row (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: Undirected weighted KSHORTEST (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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

    Scenario: Weighted KSHORTEST inside a COUNT subquery
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
            RETURN COUNT { MATCH p=(s)-[:R *KSHORTEST|4 (e, n | e.w) (e, n | n.id <> 'B')]->(t) } AS c
            """
        Then the result should be:
            | c |
            | 3 |

    Scenario: Weighted KSHORTEST inside a COUNT subquery (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            RETURN COUNT { MATCH p=(s)-[:R *KSHORTEST|4 (e, n | e.w) (e, n | n.id <> 'B')]->(t) } AS c
            """
        Then the result should be:
            | c |
            | 3 |

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

    Scenario: A constant weight with a filter is the filter-only spelling (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST|3 (e, n | 2) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path            | total |
            | ['A', 'B', 'D'] | 4     |
            | ['A', 'C', 'D'] | 4     |
            | ['A', 'D']      | 2     |

    Scenario: A constant weight with a limit keeps the fewest hops (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST|3 (e, n | 2) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path            | total |
            | ['A', 'B', 'D'] | 4     |
            | ['A', 'C', 'D'] | 4     |
            | ['A', 'D']      | 2     |

    Scenario: A zero constant weight still orders by hops
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
            MATCH p=(s)-[:R *KSHORTEST|1 (e, n | 0) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path       | total |
            | ['A', 'D'] | 0     |

    Scenario: A zero constant weight still orders by hops (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST|1 (e, n | 0) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path       | total |
            | ['A', 'D'] | 0     |

    Scenario: A constant weight with an upper bound keeps the short paths
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
            MATCH p=(s)-[:R *KSHORTEST ..2 (e, n | 1) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path            | total |
            | ['A', 'B', 'D'] | 2     |
            | ['A', 'C', 'D'] | 2     |
            | ['A', 'D']      | 1     |

    Scenario: A constant weight with an upper bound keeps the short paths (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST ..2 (e, n | 1) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path            | total |
            | ['A', 'D']      | 1     |
            | ['A', 'C', 'D'] | 2     |
            | ['A', 'B', 'D'] | 2     |

    Scenario: A constant weight with a lower bound keeps the long paths
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
            MATCH p=(s)-[:R *KSHORTEST 3.. (e, n | 1) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['A', 'E', 'F', 'D'] | 3     |
            | ['A', 'C', 'B', 'D'] | 3     |
            | ['A', 'B', 'C', 'D'] | 3     |

    Scenario: A constant weight with a lower bound keeps the long paths (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST 3.. (e, n | 1) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['A', 'C', 'B', 'D'] | 3     |
            | ['A', 'B', 'C', 'D'] | 3     |
            | ['A', 'E', 'F', 'D'] | 3     |

    Scenario: Undirected constant-weight KSHORTEST
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | 1) total]-(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                           | total |
            | ['E', 'A', 'B']                | 2     |
            | ['E', 'A', 'D', 'C', 'B']      | 4     |
            | ['E', 'A', 'D', 'C', 'B']      | 4     |
            | ['E', 'A', 'D', 'B']           | 3     |
            | ['E', 'A', 'C', 'B']           | 3     |
            | ['E', 'A', 'C', 'D', 'B']      | 4     |
            | ['E', 'A', 'C', 'B']           | 3     |
            | ['E', 'F', 'D', 'C', 'B']      | 4     |
            | ['E', 'F', 'D', 'C', 'B']      | 4     |
            | ['E', 'F', 'D', 'C', 'A', 'B'] | 5     |
            | ['E', 'F', 'D', 'B']           | 3     |
            | ['E', 'F', 'D', 'A', 'B']      | 4     |
            | ['E', 'F', 'D', 'A', 'C', 'B'] | 5     |
            | ['E', 'F', 'D', 'A', 'C', 'B'] | 5     |

    Scenario: Undirected constant-weight KSHORTEST (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | 1) total]-(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                           | total |
            | ['E', 'A', 'B']                | 2     |
            | ['E', 'A', 'D', 'C', 'B']      | 4     |
            | ['E', 'A', 'D', 'C', 'B']      | 4     |
            | ['E', 'A', 'D', 'B']           | 3     |
            | ['E', 'A', 'C', 'D', 'B']      | 4     |
            | ['E', 'A', 'C', 'B']           | 3     |
            | ['E', 'A', 'C', 'B']           | 3     |
            | ['E', 'F', 'D', 'C', 'B']      | 4     |
            | ['E', 'F', 'D', 'C', 'A', 'B'] | 5     |
            | ['E', 'F', 'D', 'C', 'B']      | 4     |
            | ['E', 'F', 'D', 'A', 'B']      | 4     |
            | ['E', 'F', 'D', 'A', 'C', 'B'] | 5     |
            | ['E', 'F', 'D', 'A', 'C', 'B'] | 5     |
            | ['E', 'F', 'D', 'B']           | 3     |

    Scenario: A constant weight runs once per upstream row
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
            UNWIND ['A', 'B', 'C'] AS sid
            MATCH (s:N {id: sid}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | 1) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['A', 'C', 'B', 'D'] | 3     |
            | ['A', 'B', 'D']      | 2     |
            | ['A', 'E', 'F', 'D'] | 3     |
            | ['A', 'D']           | 1     |
            | ['A', 'C', 'D']      | 2     |
            | ['A', 'B', 'C', 'D'] | 3     |
            | ['B', 'D']           | 1     |
            | ['B', 'C', 'D']      | 2     |
            | ['C', 'B', 'D']      | 2     |
            | ['C', 'D']           | 1     |

    Scenario: A constant weight runs once per upstream row (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            UNWIND ['A', 'B', 'C'] AS sid
            MATCH (s:N {id: sid}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | 1) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['A', 'C', 'D']      | 2     |
            | ['A', 'B', 'C', 'D'] | 3     |
            | ['A', 'C', 'B', 'D'] | 3     |
            | ['A', 'B', 'D']      | 2     |
            | ['A', 'E', 'F', 'D'] | 3     |
            | ['A', 'D']           | 1     |
            | ['B', 'C', 'D']      | 2     |
            | ['B', 'D']           | 1     |
            | ['C', 'D']           | 1     |
            | ['C', 'B', 'D']      | 2     |

    Scenario: A constant weight with a filter inside a COUNT subquery
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
            RETURN COUNT { MATCH p=(s)-[:R *KSHORTEST (e, n | 1) (e, n | n.id <> 'B')]->(t) } AS c
            """
        Then the result should be:
            | c |
            | 3 |

    Scenario: A constant weight with a filter inside a COUNT subquery (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            RETURN COUNT { MATCH p=(s)-[:R *KSHORTEST (e, n | 1) (e, n | n.id <> 'B')]->(t) } AS c
            """
        Then the result should be:
            | c |
            | 3 |

    Scenario: A positive constant parameter is added once per hop
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | $c) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['A', 'E', 'F', 'D'] | 9     |
            | ['A', 'B', 'D']      | 6     |
            | ['A', 'C', 'B', 'D'] | 9     |
            | ['A', 'D']           | 3     |
            | ['A', 'C', 'D']      | 6     |
            | ['A', 'B', 'C', 'D'] | 9     |

    Scenario: A positive constant parameter is added once per hop (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | $c) total]->(t) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['A', 'C', 'B', 'D'] | 9     |
            | ['A', 'B', 'D']      | 6     |
            | ['A', 'B', 'C', 'D'] | 9     |
            | ['A', 'C', 'D']      | 6     |
            | ['A', 'D']           | 3     |
            | ['A', 'E', 'F', 'D'] | 9     |

    Scenario: A constant weight serves whole hop classes in hop order
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
            MATCH p=(s)-[:R *KSHORTEST|3 (e, n | 1)]->(t) RETURN collect(size(relationships(p))) AS hops
            """
        Then the result should be:
            | hops      |
            | [1, 2, 2] |

    Scenario: A constant weight serves whole hop classes in hop order (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST|3 (e, n | 1)]->(t) RETURN collect(size(relationships(p))) AS hops
            """
        Then the result should be:
            | hops      |
            | [1, 2, 2] |

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
            MATCH p=(t)<-[:R *KSHORTEST (e, n | e.w) total]-(s) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                                                                                            |
            | [[['D', 'C', 'A'], 2], [['D', 'F', 'E', 'A'], 2], [['D', 'C', 'B', 'A'], 6], [['D', 'B', 'C', 'A'], 7], [['D', 'B', 'A'], 10], [['D', 'A'], 20]] |

    Scenario: Weighted KSHORTEST written right to left (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(t)<-[:R *KSHORTEST (e, n | e.w) total]-(s) RETURN collect([[x IN nodes(p) | x.id], total]) AS paths
            """
        Then the result should be:
            | paths                                                                                                                                            |
            | [[['D', 'C', 'A'], 2], [['D', 'F', 'E', 'A'], 2], [['D', 'C', 'B', 'A'], 6], [['D', 'B', 'C', 'A'], 7], [['D', 'B', 'A'], 10], [['D', 'A'], 20]] |

    Scenario: Constant-weight KSHORTEST written right to left
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
            MATCH p=(t)<-[:R *KSHORTEST (e, n | 1) total]-(s) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['D', 'B', 'A']      | 2     |
            | ['D', 'B', 'C', 'A'] | 3     |
            | ['D', 'C', 'A']      | 2     |
            | ['D', 'C', 'B', 'A'] | 3     |
            | ['D', 'A']           | 1     |
            | ['D', 'F', 'E', 'A'] | 3     |

    Scenario: Constant-weight KSHORTEST written right to left (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(t)<-[:R *KSHORTEST (e, n | 1) total]-(s) RETURN [x IN nodes(p) | x.id] AS path, total
            """
        Then the result should be:
            | path                 | total |
            | ['D', 'C', 'A']      | 2     |
            | ['D', 'C', 'B', 'A'] | 3     |
            | ['D', 'B', 'A']      | 2     |
            | ['D', 'B', 'C', 'A'] | 3     |
            | ['D', 'A']           | 1     |
            | ['D', 'F', 'E', 'A'] | 3     |

    Scenario: A v3.13 filter-only spelling now fails as a boolean weight
        # Memgraph-only syntax: a boolean weight is refused with a message that names (e, n | 1) (e, n | <filter>)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | n.id <> 'C')]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A v3.13 filter-only spelling now fails as a boolean weight (indexes :N(id))
        # Memgraph-only syntax: a boolean weight is refused with a message that names (e, n | 1) (e, n | <filter>)
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | n.id <> 'C')]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A constant weight counts paths like hop count
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | 1)]->(t) RETURN count(p) AS c
            """
        Then the result should be:
            | c |
            | 6 |

    Scenario: A constant weight counts paths like hop count (indexes :N(id))
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | 1)]->(t) RETURN count(p) AS c
            """
        Then the result should be:
            | c |
            | 6 |

    Scenario: A boolean constant weight fails
        # Memgraph-only syntax: same stripped query, so a cache hit on the plan of the scenario before it
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | true)]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A boolean constant weight fails (indexes :N(id))
        # Memgraph-only syntax: same stripped query, so a cache hit on the plan of the scenario before it
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | true)]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A negative constant weight fails
        # Memgraph-only syntax: WSHORTEST refuses negative weights
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | -1)]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A negative constant weight fails (indexes :N(id))
        # Memgraph-only syntax: WSHORTEST refuses negative weights
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | -1)]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A null weight fails
        # Memgraph-only syntax: KSHORTEST throws on null where WSHORTEST ranks null as the minimum
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.missing)]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A null weight fails (indexes :N(id))
        # Memgraph-only syntax: KSHORTEST throws on null where WSHORTEST ranks null as the minimum
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
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
            MATCH p=(s)-[:R *KSHORTEST (e, n | e.missing)]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A null constant weight fails
        # Memgraph-only syntax: the constant path must validate like the weighted one
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
            | c | null |
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | $c)]->(t) RETURN count(p) AS c
            """
        Then an error should be raised

    Scenario: A null constant weight fails (indexes :N(id))
        # Memgraph-only syntax: the constant path must validate like the weighted one
        # Pre-flight: new
        Given an empty graph
        And with new index :N(id)
        And having executed:
            """
            CREATE (a:N {id: 'A', cost: 0}), (b:N {id: 'B', cost: 9}), (c:N {id: 'C', cost: 1}), (d:N {id: 'D', cost: 1}),
                   (e:N {id: 'E', cost: 1}), (f:N {id: 'F', cost: 0}),
                   (a)-[:R {w: 5}]->(b), (b)-[:R {w: 5}]->(d), (a)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d),
                   (a)-[:R {w: 20}]->(d), (c)-[:R {w: 1}]->(b), (b)-[:R {w: 0}]->(c),
                   (a)-[:R {w: 1}]->(e), (e)-[:R {w: 0}]->(f), (f)-[:R {w: 1}]->(d)
            """
        And parameters are:
            | c | null |
        When executing query:
            """
            MATCH (s:N {id: 'A'}), (t:N {id: 'D'}) WITH s, t
            MATCH p=(s)-[:R *KSHORTEST (e, n | $c)]->(t) RETURN count(p) AS c
            """
        Then an error should be raised
