Feature: Cartesian

    Scenario: Match multiple patterns 01
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (a)-[:X]->(b), (c)-[:X]->(a)
            """
        When executing query:
            """
            MATCH (a)-[]->(), (b) CREATE (a)-[r:R]->(b) RETURN a, b, r
            """
        Then the result should be:
            | a    | b    | r    |
            | (:C) | (:A) | [:R] |
            | (:C) | (:B) | [:R] |
            | (:C) | (:C) | [:R] |
            | (:A) | (:A) | [:R] |
            | (:A) | (:B) | [:R] |
            | (:A) | (:C) | [:R] |

    Scenario: Match multiple patterns 02
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (d:D), (e:E), (f:F), (a)-[:X]->(b), (b)-[:X]->(c), (d)-[:X]->(e), (e)-[:X]->(f)
            """
        When executing query:
            """
            MATCH (a:B)--(b), (c:E)--(d) CREATE (b)-[r:R]->(d) return b, d, r
            """
        Then the result should be:
            | b    | d    | r    |
            | (:A) | (:D) | [:R] |
            | (:A) | (:F) | [:R] |
            | (:C) | (:D) | [:R] |
            | (:C) | (:F) | [:R] |

    Scenario: Match multiple patterns 03
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (d:D), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d)
            """
        When executing query:
            """
            MATCH (a:B)--(b), (c:B)--(d) RETURN b, d
            """
        Then the result should be:
            | b    | d    |
            | (:A) | (:C) |
            | (:C) | (:A) |

    Scenario: Match multiple patterns 04
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (d:D), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d)
            """
        When executing query:
            """
            MATCH (a:A)--(b), (c:A)--(d) RETURN a, b, c, d
            """
        Then the result should be empty

    Scenario: Multiple match 01
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (d:D), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d)
            """
        When executing query:
            """
            MATCH (a:B)--(b) MATCH (c:B)--(d) RETURN b, d
            """
        Then the result should be:
            | b    | d    |
            | (:A) | (:A) |
            | (:A) | (:C) |
            | (:C) | (:A) |
            | (:C) | (:C) |

    Scenario: Multiple match 02
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (d:D), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d)
            """
        When executing query:
            """
            MATCH (a:A)--(b) MATCH (a)--(c) RETURN a, b, c
            """
        Then the result should be:
            | a    | b    | c    |
            | (:A) | (:B) | (:B) |

    Scenario: Multiple match 03
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (a)-[:X]->(b), (c)-[:X]->(a)
            """
        When executing query:
            """
            MATCH (a)-[]->() MATCH (b) CREATE (a)-[r:R]->(b) RETURN a, b, r
            """
        Then the result should be:
            | a    | b    | r    |
            | (:C) | (:A) | [:R] |
            | (:C) | (:B) | [:R] |
            | (:C) | (:C) | [:R] |
            | (:A) | (:A) | [:R] |
            | (:A) | (:B) | [:R] |
            | (:A) | (:C) | [:R] |

    Scenario: Multiple match 04
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (d:D), (e:E), (f:F), (a)-[:X]->(b), (b)-[:X]->(c), (d)-[:X]->(e), (e)-[:X]->(f)
            """
        When executing query:
            """
            MATCH (a:B)--(b) MATCH (c:E)--(d) CREATE (b)-[r:R]->(d) return b, d, r
            """
        Then the result should be:
            | b    | d    | r    |
            | (:A) | (:D) | [:R] |
            | (:A) | (:F) | [:R] |
            | (:C) | (:D) | [:R] |
            | (:C) | (:F) | [:R] |

    Scenario: Multiple match 05
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C)
            """
        When executing query:
            """
            MATCH(a) MATCH(a) RETURN a
            """
        Then the result should be:
            | a    |
            | (:A) |
            | (:B) |
            | (:C) |

    Scenario: Multiple match 06
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (a)-[:R]->(b), (b)-[:R]->(c)
            """
        When executing query:
            """
            MATCH (a)-[]->() MATCH (a:B) MATCH (b:C) RETURN a, b
            """
        Then the result should be:
            | a    | b    |
            | (:B) | (:C) |

    Scenario: Multiple match 07
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C), (a)-[:R]->(b), (b)-[:R]->(c)
            """
        When executing query:
            """
            MATCH (a)-[]->() MATCH (a:B) MATCH (a:C) RETURN a
            """
        Then the result should be empty
