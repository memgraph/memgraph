Feature: Indexed label disjunction scan

    Scenario: A label disjunction keeps every upstream row (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                                   |
            | 1 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc'] |
            | 2 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc'] |

    Scenario: A label disjunction keeps equal upstream rows apart (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 1] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) RETURN count(*) AS c
            """
        Then the result should be:
            | c  |
            | 12 |

    Scenario: A WHERE label disjunction keeps every upstream row (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n) WHERE n:A OR n:B WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                                   |
            | 1 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc'] |
            | 2 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc'] |

    Scenario: A disjunction of three labels keeps every upstream row (indexes :A, :B, :C)
        Given an empty graph
        And with new index :A
        And with new index :B
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B|C) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                                         |
            | 1 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc', 'c1'] |
            | 2 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc', 'c1'] |

    Scenario: A disjunction with a negated conjunct keeps every upstream row (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:(A|B)&!C) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 1 | ['a1', 'a2', 'ab', 'b1'] |
            | 2 | ['a1', 'a2', 'ab', 'b1'] |

    Scenario: A conjunction of two disjunctions keeps every upstream row (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:(A|B)&(B|C)) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 1 | ['ab', 'ac', 'b1', 'bc'] |
            | 2 | ['ab', 'ac', 'b1', 'bc'] |

    Scenario: A conjunction of two disjunctions keeps every upstream row (indexes :A, :B, :C)
        Given an empty graph
        And with new index :A
        And with new index :B
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:(A|B)&(B|C)) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 1 | ['ab', 'ac', 'b1', 'bc'] |
            | 2 | ['ab', 'ac', 'b1', 'bc'] |

    Scenario: A write before a label disjunction runs once (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            CREATE (w:W) WITH w MATCH (n:A|B) WITH count(*) AS c MATCH (m:W) RETURN c, count(m) AS ws
            """
        Then the result should be:
            | c | ws |
            | 6 | 1  |

    Scenario: A write after a label disjunction runs once per node (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (n:A|B) CREATE (n)-[:T]->(:M) WITH count(*) AS c MATCH (m:M) RETURN c, count(m) AS ms
            """
        Then the result should be:
            | c | ms |
            | 6 | 6  |

    Scenario: A property disjunction bound to an upstream value seeks per row (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2, 3] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B {p: x}) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 1 | ['a1', 'b1'] |
            | 2 | ['a2', 'ab'] |
            | 3 | ['ac', 'bc'] |

    Scenario: A property disjunction bound to an upstream value seeks per row (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2, 3] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B {p: x}) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 1 | ['a1', 'b1'] |
            | 2 | ['a2', 'ab'] |
            | 3 | ['ac', 'bc'] |

    Scenario: A property disjunction bound to an upstream value seeks per row (indexes :A, :B(p), :C)
        Given an empty graph
        And with new index :A
        And with new index :B(p)
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2, 3] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B {p: x}) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 1 | ['a1', 'b1'] |
            | 2 | ['a2', 'ab'] |
            | 3 | ['ac', 'bc'] |

    Scenario: A null upstream value matches no property (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [null, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B {p: x}) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 2 | ['a2', 'ab'] |

    Scenario: A null upstream value matches no property (indexes :A, :B(p), :C)
        Given an empty graph
        And with new index :A
        And with new index :B(p)
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [null, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B {p: x}) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 2 | ['a2', 'ab'] |

    Scenario: A range disjunction bound to an upstream value seeks per row (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [2, 3] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WHERE n.p >= x WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 2 | ['a2', 'ab', 'ac', 'bc'] |
            | 3 | ['ac', 'bc']             |

    Scenario: A range disjunction bound to an upstream value seeks per row (indexes :A, :B(p), :C)
        Given an empty graph
        And with new index :A
        And with new index :B(p)
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [2, 3] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WHERE n.p >= x WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 2 | ['a2', 'ab', 'ac', 'bc'] |
            | 3 | ['ac', 'bc']             |

    Scenario: A range disjunction bound to an upstream value seeks per row (indexes :A(p) WITH CONFIG {"order": "DESC"}, :B(p) WITH CONFIG {"order": "DESC"}, :C(p) WITH CONFIG {"order": "DESC"})
        Given an empty graph
        And with new index :A(p) WITH CONFIG {"order": "DESC"}
        And with new index :B(p) WITH CONFIG {"order": "DESC"}
        And with new index :C(p) WITH CONFIG {"order": "DESC"}
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [2, 3] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WHERE n.p >= x WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 2 | ['a2', 'ab', 'ac', 'bc'] |
            | 3 | ['ac', 'bc']             |

    Scenario: An IN list over an upstream value seeks per row (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WHERE n.p IN [x, 3] WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 1 | ['a1', 'ac', 'b1', 'bc'] |
            | 2 | ['a2', 'ab', 'ac', 'bc'] |

    Scenario: An IN list over an upstream value seeks per row (indexes :A, :B(p), :C)
        Given an empty graph
        And with new index :A
        And with new index :B(p)
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WHERE n.p IN [x, 3] WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 1 | ['a1', 'ac', 'b1', 'bc'] |
            | 2 | ['a2', 'ab', 'ac', 'bc'] |

    Scenario: An IN list over an upstream value seeks per row (indexes :A(p) WITH CONFIG {"order": "DESC"}, :B(p) WITH CONFIG {"order": "DESC"}, :C(p) WITH CONFIG {"order": "DESC"})
        Given an empty graph
        And with new index :A(p) WITH CONFIG {"order": "DESC"}
        And with new index :B(p) WITH CONFIG {"order": "DESC"}
        And with new index :C(p) WITH CONFIG {"order": "DESC"}
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WHERE n.p IN [x, 3] WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                       |
            | 1 | ['a1', 'ac', 'b1', 'bc'] |
            | 2 | ['a2', 'ab', 'ac', 'bc'] |

    Scenario: An IN list at the start of a query returns each node once (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | ps | [1, 1, 2] |
        When executing query:
            """
            MATCH (n:A|B) WHERE n.p IN $ps RETURN n.n AS v ORDER BY v
            """
        Then the result should be, in order:
            | v    |
            | 'a1' |
            | 'a2' |
            | 'ab' |
            | 'b1' |

    Scenario: An empty IN list matches nothing (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | ps | [] |
        When executing query:
            """
            MATCH (n:A|B) WHERE n.p IN $ps RETURN n.n AS v ORDER BY v
            """
        Then the result should be empty

    Scenario: A property disjunction with a residual filter keeps it (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B {p: x}) WHERE n.n <> 'ab' WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 1 | ['a1', 'b1'] |
            | 2 | ['a2']       |

    Scenario: A property disjunction with a residual filter keeps it (indexes :A, :B(p), :C)
        Given an empty graph
        And with new index :A
        And with new index :B(p)
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B {p: x}) WHERE n.n <> 'ab' WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 1 | ['a1', 'b1'] |
            | 2 | ['a2']       |

    Scenario: A label disjunction as a variable-length destination keeps every source (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (z:Z) WITH z MATCH (z)-[*1..2]->(n:A|B) RETURN z.n AS z, n.n AS v ORDER BY z, v
            """
        Then the result should be, in order:
            | z    | v    |
            | 'z1' | 'a1' |
            | 'z1' | 'ab' |
            | 'z2' | 'ab' |
            | 'z2' | 'b1' |
            | 'z2' | 'bc' |

    Scenario: A label disjunction as a weighted shortest destination keeps every source (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (z:Z) WITH z MATCH (z)-[*wShortest 3 (r, m | 1) w]->(n:A|B) RETURN z.n AS z, n.n AS v ORDER BY z, v
            """
        Then the result should be, in order:
            | z    | v    |
            | 'z1' | 'a1' |
            | 'z1' | 'ab' |
            | 'z2' | 'ab' |
            | 'z2' | 'b1' |
            | 'z2' | 'bc' |

    Scenario: A label disjunction as an all-shortest destination keeps every source (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (z:Z) WITH z MATCH (z)-[*allShortest 3 (r, m | 1) w]->(n:A|B) RETURN z.n AS z, n.n AS v ORDER BY z, v
            """
        Then the result should be, in order:
            | z    | v    |
            | 'z1' | 'a1' |
            | 'z1' | 'ab' |
            | 'z2' | 'ab' |
            | 'z2' | 'b1' |
            | 'z2' | 'bc' |

    Scenario: A label disjunction after a bound match multiplies its rows (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (m:C) WITH m MATCH (n:A|B) RETURN count(*) AS c
            """
        Then the result should be:
            | c  |
            | 18 |

    Scenario: A label disjunction in a second pattern part is a cross product (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (m:Z), (n:A|B) RETURN count(*) AS c
            """
        Then the result should be:
            | c  |
            | 12 |

    Scenario: An optional label disjunction keeps rows with no match (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2, 9] |
        When executing query:
            """
            UNWIND $xs AS x OPTIONAL MATCH (n:A|B {p: x}) RETURN x, count(n) AS c ORDER BY x
            """
        Then the result should be, in order:
            | x | c |
            | 1 | 2 |
            | 2 | 2 |
            | 9 | 0 |

    Scenario: A label disjunction in a COUNT subquery runs per outer row (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2, 3] |
        When executing query:
            """
            UNWIND $xs AS x RETURN x, COUNT { MATCH (n:A|B) WHERE n.p = x } AS c ORDER BY x
            """
        Then the result should be, in order:
            | x | c |
            | 1 | 2 |
            | 2 | 2 |
            | 3 | 2 |

    Scenario: A label disjunction under LIMIT keeps the upstream order (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:A|B) WITH x, n.n AS v ORDER BY x, v LIMIT 7 RETURN x, v
            """
        Then the result should be, in order:
            | x | v    |
            | 1 | 'a1' |
            | 1 | 'a2' |
            | 1 | 'ab' |
            | 1 | 'ac' |
            | 1 | 'b1' |
            | 1 | 'bc' |
            | 2 | 'a1' |

    Scenario: A label disjunction after an ordered limit keeps the chosen row (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (z:Z) WITH z ORDER BY z.n DESC LIMIT 1 MATCH (n:A|B) RETURN z.n AS z, count(*) AS c
            """
        Then the result should be, in order:
            | z    | c |
            | 'z2' | 6 |

    Scenario: A seek on a variable-length edge list does not prune the expansion (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (a:Z)-[r*1..2]->(b) UNWIND [1] AS x MATCH (n:A|B) WHERE n.p = size(r) RETURN count(DISTINCT n) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: A seek on a variable-length edge list does not prune the expansion (indexes :A, :B(p), :C)
        Given an empty graph
        And with new index :A
        And with new index :B(p)
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (a:Z)-[r*1..2]->(b) UNWIND [1] AS x MATCH (n:A|B) WHERE n.p = size(r) RETURN count(DISTINCT n) AS c
            """
        Then the result should be:
            | c |
            | 4 |

    Scenario: Two label disjunctions joined on a property (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (n:A|B), (m:A|B) WHERE n.p = m.p RETURN count(*) AS c
            """
        Then the result should be:
            | c  |
            | 12 |

    Scenario: Two label disjunctions joined on a property (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (n:A|B), (m:A|B) WHERE n.p = m.p RETURN count(*) AS c
            """
        Then the result should be:
            | c  |
            | 12 |

    Scenario: A label removed after the scan does not repeat a node (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (o:O) SET o.x = 1 WITH o MATCH (n:A|B) REMOVE n:A RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 6 |

    Scenario: A label added to a later node after the scan does not drop it (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (o:O) SET o.x = 1 WITH o MATCH (n:A|B) WITH n ORDER BY n.n MATCH (k:B {n: 'b1'}) SET k:A RETURN count(*) AS c
            """
        Then the result should be, in order:
            | c |
            | 6 |

    Scenario: A node deleted after the scan is counted once (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        When executing query:
            """
            MATCH (o:O) SET o.x = 1 WITH o MATCH (n:A|B) DETACH DELETE n RETURN count(*) AS c
            """
        Then the result should be:
            | c |
            | 6 |

    Scenario: An IN operand that is not a list raises (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | v | 5 |
        When executing query:
            """
            MATCH (n:A|B) WHERE n.p IN $v RETURN n.n AS v
            """
        Then an error should be raised

    Scenario: A null IN operand matches nothing (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | v | null |
        When executing query:
            """
            MATCH (n:A|B) WHERE n.p IN $v RETURN n.n AS v
            """
        Then the result should be empty

    Scenario: A null IN element skips only that element (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | v | [null, 1] |
        When executing query:
            """
            MATCH (n:A|B) WHERE n.p IN $v RETURN n.n AS v ORDER BY v
            """
        Then the result should be, in order:
            | v    |
            | 'a1' |
            | 'b1' |

    Scenario: A subsumed disjunction keeps every upstream row (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x MATCH (n:(A|B)&(A|B|C)) WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs                                   |
            | 1 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc'] |
            | 2 | ['a1', 'a2', 'ab', 'ac', 'b1', 'bc'] |

    Scenario: A label disjunction after WITH WHERE keeps every row (indexes :A, :B)
        Given an empty graph
        And with new index :A
        And with new index :B
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x WITH x WHERE x > 0 MATCH (n:A|B) WHERE n.p = x WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 1 | ['a1', 'b1'] |
            | 2 | ['a2', 'ab'] |

    Scenario: A label disjunction after WITH WHERE keeps every row (indexes :A, :B(p), :C)
        Given an empty graph
        And with new index :A
        And with new index :B(p)
        And with new index :C
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2] |
        When executing query:
            """
            UNWIND $xs AS x WITH x WHERE x > 0 MATCH (n:A|B) WHERE n.p = x WITH x, n.n AS v ORDER BY x, v RETURN x, collect(v) AS vs ORDER BY x
            """
        Then the result should be, in order:
            | x | vs           |
            | 1 | ['a1', 'b1'] |
            | 2 | ['a2', 'ab'] |

    Scenario: A label disjunction in a CALL subquery runs per outer row (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 2, 3] |
        When executing query:
            """
            UNWIND $xs AS x CALL { WITH x MATCH (n:A|B) WHERE n.p = x RETURN count(*) AS c } RETURN x, c ORDER BY x
            """
        Then the result should be, in order:
            | x | c |
            | 1 | 2 |
            | 2 | 2 |
            | 3 | 2 |

    Scenario: A label disjunction in EXISTS runs per outer row (indexes :A(p), :B(p), :C(p))
        Given an empty graph
        And with new index :A(p)
        And with new index :B(p)
        And with new index :C(p)
        And having executed:
            """
            CREATE (a1:A {n: 'a1', p: 1}), (a2:A {n: 'a2', p: 2}), (b1:B {n: 'b1', p: 1}), (ab:A:B {n: 'ab', p: 2}),
                   (c1:C {n: 'c1', p: 1}), (ac:A:C {n: 'ac', p: 3}), (bc:B:C {n: 'bc', p: 3}), (o:O {n: 'o', p: 1}),
                   (z1:Z {n: 'z1'}), (z2:Z {n: 'z2'})
            CREATE (z1)-[:R]->(a1), (z1)-[:R]->(ab), (z2)-[:R]->(ab), (z2)-[:R]->(b1), (z2)-[:R]->(bc), (ab)-[:R]->(c1)
            """
        And parameters are:
            | xs | [1, 3, 9] |
        When executing query:
            """
            UNWIND $xs AS x RETURN x, EXISTS { MATCH (n:A|B) WHERE n.p = x } AS e ORDER BY x
            """
        Then the result should be, in order:
            | x | e     |
            | 1 | true  |
            | 3 | true  |
            | 9 | false |
