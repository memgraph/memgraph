Feature: Aggregations

    Scenario: Count test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C)
            """
        When executing query:
            """
            MATCH (a) RETURN COUNT(a) AS n
            """
        Then the result should be:
            | n |
            | 3 |

    Scenario: Count test 02:
        Given an empty graph
        When executing query:
            """
            RETURN COUNT(123) AS n
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Count test 03:
        Given an empty graph
        When executing query:
            """
            RETURN COUNT(true) AS n
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Count test 04:
        Given an empty graph
        When executing query:
            """
            RETURN COUNT('abcd') AS n
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Count test 05:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0}), (b{x: 0}), (c{x: 0}), (d{x: 1}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN COUNT(a) AS n, a.x
            """
        Then the result should be:
            | n | a.x |
            | 3 | 0   |
            | 2 | 1   |

    Scenario: Sum test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN SUM(a.x) AS n
            """
        Then an error should be raised

    Scenario: Sum test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b), (c{x: 5}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN SUM(a.x) AS n
            """
        Then the result should be:
            | n |
            | 6 |

    Scenario: Sum test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN SUM(a.y) AS n, a.x
            """
        Then the result should be:
            | n | a.x |
            | 4 | 0   |
            | 4 | 1   |

    Scenario: Avg test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN AVG(a.x) AS n
            """
        Then an error should be raised

    Scenario: Avg test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1.25}), (b), (c{x: 4.75}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN AVG(a.x) AS n
            """
        Then the result should be:
            | n   |
            | 3.0 |

    Scenario: Avg test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN AVG(a.y) AS n, a.x
            """
        Then the result should be:
            | n   | a.x |
            | 2.0 | 0   |
            | 4.0 | 1   |

    Scenario: Min test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN MIN(a.x) AS n
            """
        Then an error should be raised

    Scenario: Min test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN MIN(a.x) AS n
            """
        Then the result should be:
            | n  |
            | 1  |

    Scenario: Min test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN MIN(a.y) AS n, a.x
            """
        Then the result should be:
            | n | a.x |
            | 1 | 0   |
            | 4 | 1   |

    Scenario: Max test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN MAX(a.x) AS n
            """
        Then an error should be raised

    Scenario: Max test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN MAX(a.x) AS n
            """
        Then the result should be:
            | n  |
            | 9  |

    Scenario: Max test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN Max(a.y) AS n, a.x
            """
        Then the result should be:
            | n | a.x |
            | 3 | 0   |
            | 4 | 1   |

    Scenario: Collect test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0}), (b{x: True}), (c{x: 'asdf'})
            """
        When executing query:
            """
            MATCH (a) RETURN collect(a.x) AS n
            """
        Then the result should be (ignoring element order for lists)
            | n                 |
            | [0, true, 'asdf'] |

    Scenario: Collect test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0}), (b{x: True}), (c{x: 'asdf'}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN collect(a.x) AS n
            """
        Then the result should be (ignoring element order for lists)
            | n                 |
            | [0, true, 'asdf'] |
