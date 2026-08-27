Feature: Order by

    Scenario: Test NaN sorts after every other number
        Given an empty graph
        When executing query:
            """
            UNWIND [3.0, 0.0/0.0, 1.0, 2.0] AS x
            WITH x ORDER BY x
            RETURN CASE WHEN x <> x THEN 'NaN' ELSE toString(x) END AS v
            """
        Then the result should be, in order:
            | v     |
            | '1'   |
            | '2'   |
            | '3'   |
            | 'NaN' |

    Scenario: Test NaN sorts after positive infinity
        Given an empty graph
        When executing query:
            """
            UNWIND [1.0 / 0.0, 0.0 / 0.0, -1.0 / 0.0, 0.0] AS x
            WITH x ORDER BY x
            RETURN CASE WHEN x <> x THEN 'NaN' ELSE toString(x) END AS v
            """
        Then the result should be, in order:
            | v     |
            | '-inf' |
            | '0'   |
            | 'inf' |
            | 'NaN' |

    Scenario: Test NaN sorts before null
        Given an empty graph
        When executing query:
            """
            UNWIND [1.0, 0.0 / 0.0, null, 2.0] AS x
            WITH x ORDER BY x
            RETURN CASE WHEN x IS NULL THEN 'NULL' WHEN x <> x THEN 'NaN' ELSE toString(x) END AS v
            """
        Then the result should be, in order:
            | v      |
            | '1'    |
            | '2'    |
            | 'NaN'  |
            | 'NULL' |

    Scenario: Test descending reverses the order NaN and null sit in
        Given an empty graph
        When executing query:
            """
            UNWIND [1.0, 0.0 / 0.0, null, 2.0] AS x
            WITH x ORDER BY x DESC
            RETURN CASE WHEN x IS NULL THEN 'NULL' WHEN x <> x THEN 'NaN' ELSE toString(x) END AS v
            """
        Then the result should be, in order:
            | v      |
            | 'NULL' |
            | 'NaN'  |
            | '2'    |
            | '1'    |

    Scenario: Test rows holding no NaN are ordered among themselves
        Given an empty graph
        When executing query:
            """
            UNWIND [5.0, 3.0, 0.0 / 0.0, 9.0, 1.0, 7.0, 2.0, 8.0, 4.0, 6.0] AS x
            WITH x ORDER BY x
            RETURN CASE WHEN x <> x THEN 'NaN' ELSE toString(x) END AS v
            """
        Then the result should be, in order:
            | v     |
            | '1'   |
            | '2'   |
            | '3'   |
            | '4'   |
            | '5'   |
            | '6'   |
            | '7'   |
            | '8'   |
            | '9'   |
            | 'NaN' |

    Scenario: Test LIMIT returns the rows that sort first
        Given an empty graph
        When executing query:
            """
            UNWIND [3.0, 0.0 / 0.0, 1.0, 2.0] AS x
            WITH x ORDER BY x LIMIT 2
            RETURN CASE WHEN x <> x THEN 'NaN' ELSE toString(x) END AS v
            """
        Then the result should be, in order:
            | v   |
            | '1' |
            | '2' |

    Scenario: Test a NaN in the first sort key leaves the second key ordered
        Given an empty graph
        When executing query:
            """
            UNWIND [[1, 0.0 / 0.0], [1, 2.0], [1, 1.0], [1, 3.0]] AS p
            WITH p[0] AS a, p[1] AS b
            ORDER BY a, b
            RETURN CASE WHEN b <> b THEN 'NaN' ELSE toString(b) END AS v
            """
        Then the result should be, in order:
            | v     |
            | '1'   |
            | '2'   |
            | '3'   |
            | 'NaN' |

    Scenario: Test max returns NaN and min ignores it
        Given an empty graph
        When executing query:
            """
            UNWIND [1.0, 0.0 / 0.0, 3.0] AS x
            WITH min(x) AS lo, max(x) AS hi
            RETURN toString(lo) AS lo, CASE WHEN hi <> hi THEN 'NaN' ELSE toString(hi) END AS hi
            """
        Then the result should be:
            | lo  | hi    |
            | '1' | 'NaN' |

    Scenario: Test ordering a list whose elements hold a NaN
        Given an empty graph
        When executing query:
            """
            UNWIND [[1.0], [0.0 / 0.0], [2.0]] AS x
            WITH x ORDER BY x
            RETURN CASE WHEN x[0] <> x[0] THEN 'NaN' ELSE toString(x[0]) END AS v
            """
        Then the result should be, in order:
            | v     |
            | '1'   |
            | '2'   |
            | 'NaN' |

    Scenario: Test ordering without a NaN is unaffected
        Given an empty graph
        When executing query:
            """
            UNWIND [3.0, 9.0, 1.0, 2.0] AS x
            WITH x ORDER BY x
            RETURN toString(x) AS v
            """
        Then the result should be, in order:
            | v   |
            | '1' |
            | '2' |
            | '3' |
            | '9' |

    Scenario: Test values of unlike types are ordered rather than refused
        Given an empty graph
        When executing query:
            """
            UNWIND [[1, 'int'], [true, 'bool'], ['', 'string'], [3.14, 'float'],
                    [{a: 1}, 'map'], [[2], 'list'], [null, 'null']] AS p
            WITH p[0] AS v, p[1] AS tag
            ORDER BY v
            RETURN tag
            """
        Then the result should be, in order:
            | tag      |
            | 'map'    |
            | 'list'   |
            | 'string' |
            | 'bool'   |
            | 'int'    |
            | 'float'  |
            | 'null'   |

    Scenario: Test points and temporal values sit before strings in the order
        Given an empty graph
        When executing query:
            """
            UNWIND [['z', 'string'], [1, 'int'], [date('2024-01-01'), 'date'],
                    [localtime('12:00'), 'localtime'], [localdatetime('2024-01-01T12:00'), 'localdatetime'],
                    [duration('P1D'), 'duration'], [point({x: 1, y: 2}), 'point']] AS p
            WITH p[0] AS v, p[1] AS tag
            ORDER BY v
            RETURN tag
            """
        Then the result should be, in order:
            | tag             |
            | 'point'         |
            | 'localdatetime' |
            | 'date'          |
            | 'localtime'     |
            | 'duration'      |
            | 'string'        |
            | 'int'           |

    Scenario: Test descending reverses the order of unlike types
        Given an empty graph
        When executing query:
            """
            UNWIND [[1, 'int'], [true, 'bool'], ['', 'string'], [{a: 1}, 'map'], [null, 'null']] AS p
            WITH p[0] AS v, p[1] AS tag
            ORDER BY v DESC
            RETURN tag
            """
        Then the result should be, in order:
            | tag      |
            | 'null'   |
            | 'int'    |
            | 'bool'   |
            | 'string' |
            | 'map'    |

    Scenario: Test maps are ordered by their keys and then their values
        Given an empty graph
        When executing query:
            """
            UNWIND [[{b: 1}, 'b1'], [{a: 1}, 'a1'], [{a: 2}, 'a2'], [{a: 1, b: 2}, 'a1b2']] AS p
            WITH p[0] AS v, p[1] AS tag
            ORDER BY v
            RETURN tag
            """
        Then the result should be, in order:
            | tag    |
            | 'a1'   |
            | 'a1b2' |
            | 'a2'   |
            | 'b1'   |

    Scenario: Test nodes and relationships are ordered before lists
        Given an empty graph
        And having executed:
            """
            CREATE (:A)-[:R]->(:B)
            """
        When executing query:
            """
            MATCH (a:A)-[r:R]->(b:B)
            UNWIND [[a, 'node'], [r, 'relationship'], [[2], 'list'], ['z', 'string']] AS p
            WITH p[0] AS v, p[1] AS tag
            ORDER BY v
            RETURN tag
            """
        Then the result should be, in order:
            | tag            |
            | 'node'         |
            | 'relationship' |
            | 'list'         |
            | 'string'       |
