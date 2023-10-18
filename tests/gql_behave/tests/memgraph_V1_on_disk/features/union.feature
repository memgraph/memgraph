Feature: Union

    Scenario: Test return *
        Given an empty graph
        When executing query:
            """
            WITH 5 AS X, 3 AS Y RETURN * UNION WITH 9 AS X, 4 AS Y RETURN *;
            """
        Then the result should be:
            |  X  |  Y  |
            |  5  |  3  |
            |  9  |  4  |

    Scenario: Test return * with swapped parameters
        Given an empty graph
        When executing query:
            """
            WITH 5 AS X, 3 AS Y RETURN * UNION WITH 9 AS Y, 4 AS X RETURN *;
            """
        Then the result should be:
            |  X  |  Y  |
            |  5  |  3  |
            |  4  |  9  |

    Scenario: Test return * and named parameters
        Given an empty graph
        When executing query:
            """
            WITH 5 AS X, 3 AS Y RETURN * UNION WITH 9 AS Y, 4 AS X RETURN Y, X;
            """
        Then the result should be:
            |  X  |  Y  |
            |  5  |  3  |
            |  4  |  9  |

    Scenario: Test distinct single elements are returned
        Given an empty graph
        And having executed
            """
            CREATE (a: A{x: 1, y: 1}), (b: A{x: 1, y: 2}), (c: A{x: 2, y: 1}), (d: A{x: 1, y:1})
            """
        When executing query:
            """
            MATCH (a: A{x:1}) RETURN a.x AS X UNION MATCH (a: A{y:1}) RETURN a.x AS X;
            """
        Then the result should be:
            |  X  |
            |  1  |
            |  2  |

    Scenario: Test all single elements are returned
        Given an empty graph
        And having executed
            """
            CREATE (a: A{x: 1, y: 1}), (b: A{x: 1, y: 2}), (c: A{x: 2, y: 1}), (d: A{x: 1, y:1})
            """
        When executing query:
            """
            MATCH (a: A{x:1}) RETURN a.x AS X UNION ALL MATCH (a: A{y:1}) RETURN a.x AS X;
            """
        Then the result should be:
            |  X  |
            |  1  |
            |  1  |
            |  1  |
            |  1  |
            |  2  |
            |  1  |

    Scenario: Test distinct elements are returned
        Given an empty graph
        And having executed
            """
            CREATE (a: A{x: 1, y: 1}), (b: A{x: 1, y: 2}), (c: A{x: 2, y: 1}), (d: A{x: 1, y:1})
            """
        When executing query:
            """
            MATCH (a: A{x:1}) RETURN a.x AS X, a.y AS Y UNION MATCH (a: A{y:1}) RETURN a.x AS X, a.y AS Y;
            """
        Then the result should be:
            |  X  |  Y  |
            |  1  |  1  |
            |  1  |  2  |
            |  2  |  1  |

    Scenario: Test all elements are returned
        Given an empty graph
        And having executed
            """
            CREATE (a: A{x: 1, y: 1}), (b: A{x: 1, y: 2}), (c: A{x: 2, y: 1}), (d: A{x: 1, y:1})
            """
        When executing query:
            """
            MATCH (a: A{x:1}) RETURN a.x AS X, a.y AS Y UNION ALL MATCH (a: A{y:1}) RETURN a.x AS X, a.y AS Y;
            """
        Then the result should be:
            |  X  |  Y  |
            |  1  |  1  |
            |  1  |  2  |
            |  1  |  1  |
            |  1  |  1  |
            |  2  |  1  |
            |  1  |  1  |

    Scenario: Test union combinator 1
        Given an empty graph
        When executing query:
            """
            MATCH (a) RETURN a.x as V UNION ALL MATCH (a) RETURN a.y AS V UNION MATCH (a) RETURN a.y + a.x AS V;
            """
        Then an error should be raised

    Scenario: Test union combinator 2
        Given an empty graph
        When executing query:
            """
            MATCH (a) RETURN a.x as V UNION MATCH (a) RETURN a.y AS V UNION ALL MATCH (a) RETURN a.y + a.x AS V;
            """
        Then an error should be raised

    Scenario: Different names of return expressions
        Given an empty graph
        When executing query:
            """
            RETURN 10 as x UNION RETURN 11 as y
            """
        Then an error should be raised

    Scenario: Different number of return expressions
        Given an empty graph
        When executing query:
            """
            RETURN 10 as x, 11 as y UNION RETURN 11 as y
            """
        Then an error should be raised
