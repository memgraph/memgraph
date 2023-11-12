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

    Scenario: Multiple match with WHERE x = y 01
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1}), (:A {id: 2}), (:B {id: 1})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:B) WHERE a.id = b.id RETURN a, b
            """
        Then the result should be:
            | a            | b            |
            | (:A {id: 1}) | (:B {id: 1}) |

    Scenario: Multiple match with WHERE x = y 01 reversed
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1}), (:A {id: 2}), (:B {id: 1})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:B) WHERE b.id = a.id RETURN a, b
            """
        Then the result should be:
            | a            | b            |
            | (:A {id: 1}) | (:B {id: 1}) |

    Scenario: Multiple match with WHERE x = y 02
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1}), (:A {id: 2}), (:B {id: 1}), (:B {id: 2})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:B) WHERE a.id = b.id RETURN a, b
            """
        Then the result should be:
            | a            | b            |
            | (:A {id: 1}) | (:B {id: 1}) |
            | (:A {id: 2}) | (:B {id: 2}) |

    Scenario: Multiple match with WHERE x = y 03
        Given an empty graph
        And having executed
            """
            CREATE (:A {prop: 1, id: 1}), (:A {prop: 2, id: 2}), (:A {prop: 1, id: 2}), (:B {prop: 2, id: 3})
            """
        When executing query:
            """
            MATCH (a) MATCH (b) WHERE a.prop = b.prop RETURN a, b
            """
        Then the result should be:
            | a                     | b                     |
            | (:A {id: 1, prop: 1}) | (:A {id: 1, prop: 1}) |
            | (:A {id: 1, prop: 1}) | (:A {id: 2, prop: 1}) |
            | (:A {id: 2, prop: 2}) | (:A {id: 2, prop: 2}) |
            | (:A {id: 2, prop: 2}) | (:B {id: 3, prop: 2}) |
            | (:A {id: 2, prop: 1}) | (:A {id: 1, prop: 1}) |
            | (:A {id: 2, prop: 1}) | (:A {id: 2, prop: 1}) |
            | (:B {id: 3, prop: 2}) | (:A {id: 2, prop: 2}) |
            | (:B {id: 3, prop: 2}) | (:B {id: 3, prop: 2}) |

    Scenario: Multiple match with WHERE x = y 04
        Given an empty graph
        And having executed
            """
            CREATE (:A {prop: 1, id: 1}), (:A {prop: 2, id: 2}), (:A {prop: 1, id: 2}), (:B {prop: 2, id: 3})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:A) WHERE a.prop = b.prop RETURN a, b
            """
        Then the result should be:
            | a                     | b                     |
            | (:A {id: 1, prop: 1}) | (:A {id: 1, prop: 1}) |
            | (:A {id: 1, prop: 1}) | (:A {id: 2, prop: 1}) |
            | (:A {id: 2, prop: 2}) | (:A {id: 2, prop: 2}) |
            | (:A {id: 2, prop: 1}) | (:A {id: 1, prop: 1}) |
            | (:A {id: 2, prop: 1}) | (:A {id: 2, prop: 1}) |

    Scenario: Multiple match with WHERE x = y 05
        Given an empty graph
        And having executed
            """
            CREATE (:A {prop: 1, id: 1}), (:A {prop: 2, id: 2}), (:A {prop: 1, id: 2}), (:A {prop: 2, id: 3})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:A) WHERE a.prop = b.id RETURN a, b
            """
        Then the result should be:
            | a                     | b                     |
            | (:A {id: 1, prop: 1}) | (:A {id: 1, prop: 1}) |
            | (:A {id: 2, prop: 1}) | (:A {id: 1, prop: 1}) |
            | (:A {id: 2, prop: 2}) | (:A {id: 2, prop: 2}) |
            | (:A {id: 3, prop: 2}) | (:A {id: 2, prop: 2}) |
            | (:A {id: 2, prop: 2}) | (:A {id: 2, prop: 1}) |
            | (:A {id: 3, prop: 2}) | (:A {id: 2, prop: 1}) |

    Scenario: Multiple match with WHERE x = y 06
        Given an empty graph
        And having executed
            """
            CREATE (:A {prop: 1, id: 1}), (:A {prop: 2, id: 2}), (:A {prop: 1, id: 2}), (:A {prop: 2, id: 3})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:A) WHERE a.id = b.prop RETURN a, b
            """
        Then the result should be:
            | a                     | b                     |
            | (:A {id: 1, prop: 1}) | (:A {id: 1, prop: 1}) |
            | (:A {id: 2, prop: 2}) | (:A {id: 2, prop: 2}) |
            | (:A {id: 2, prop: 1}) | (:A {id: 2, prop: 2}) |
            | (:A {id: 1, prop: 1}) | (:A {id: 2, prop: 1}) |
            | (:A {id: 2, prop: 2}) | (:A {id: 3, prop: 2}) |
            | (:A {id: 2, prop: 1}) | (:A {id: 3, prop: 2}) |

    Scenario: Multiple match with WHERE x = y 07: nothing on the left side
        Given an empty graph
        And having executed
            """
            CREATE (:B {id: 1})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:B) WHERE a.id = b.id RETURN a, b
            """
        Then the result should be empty

    Scenario: Multiple match with WHERE x = y 08: nothing on the right side
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1}), (:A {id: 2})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:B) WHERE a.id = b.id RETURN a, b
            """
        Then the result should be empty

    Scenario: Multiple match with WHERE x = y 09: sides never equal
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1}), (:B {id: 2})
            """
        When executing query:
            """
            MATCH (a:A) MATCH (b:B) WHERE a.id = b.id RETURN a, b
            """
        Then the result should be empty

    Scenario: Multiple match + with 01
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1}), (:A {id: 2}), (:B {id: 1})
            """
        When executing query:
            """
            MATCH (a:A) WITH a MATCH (b:B) WHERE a.id = b.id RETURN a, b
            """
        Then the result should be:
            | a            | b            |
            | (:A {id: 1}) | (:B {id: 1}) |

    Scenario: Multiple match + with 02
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1}), (:A {id: 2}), (:B {id: 1})
            """
        When executing query:
            """
            MATCH (a:A) WITH a.id as id MATCH (a:A) return a;
            """
        Then the result should be:
            | a            |
            | (:A {id: 1}) |
            | (:A {id: 2}) |
            | (:A {id: 1}) |
            | (:A {id: 2}) |

    Scenario: Multiple match + with 03
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1})-[:TYPE]->(:B {id: 1}), (:A {id: 2})-[:TYPE]->(:B {id: 2})
            """
        When executing query:
            """
            MATCH (a:A) WITH a.id as id MATCH (a)-[:TYPE]->(b) return a, b;
            """
        Then the result should be:
            | a            | b            |
            | (:A {id: 1}) | (:B {id: 1}) |
            | (:A {id: 2}) | (:B {id: 2}) |
            | (:A {id: 1}) | (:B {id: 1}) |
            | (:A {id: 2}) | (:B {id: 2}) |

    Scenario: Multiple match + with 04
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1})-[:TYPE]->(:B {id: 1}), (:A {id: 2})-[:TYPE]->(:B {id: 2})
            """
        When executing query:
            """
            MATCH (a:A) WITH a MATCH (a)-[:TYPE]->(b) return a, b;
            """
        Then the result should be:
            | a            | b            |
            | (:A {id: 1}) | (:B {id: 1}) |
            | (:A {id: 2}) | (:B {id: 2}) |

    Scenario: Multiple match + with 05
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1})-[:TYPE]->(:B {id: 1}), (:A {id: 2})-[:TYPE]->(:B {id: 2})
            """
        When executing query:
            """
            MATCH (a:A) WITH a MATCH (c:A {id: 1}), (a)-[:TYPE]->(b) return a, b;
            """
        Then the result should be:
            | a            | b            |
            | (:A {id: 1}) | (:B {id: 1}) |
            | (:A {id: 2}) | (:B {id: 2}) |

    Scenario: Double match with Cyphermorphism
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1})-[:TYPE]->(:B {id: 1}), (:A {id: 2})-[:TYPE]->(:B {id: 2})
            """
        When executing query:
            """
            MATCH (a)-->(b), (c)-->(d) RETURN a, b, c, d
            """
        Then the result should be:
            | a            | b            | c            | d            |
            | (:A {id: 1}) | (:B {id: 1}) | (:A {id: 2}) | (:B {id: 2}) |
            | (:A {id: 2}) | (:B {id: 2}) | (:A {id: 1}) | (:B {id: 1}) |

    Scenario: Triple match with Cyphermorphism empty result
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1})-[:TYPE]->(:B {id: 1}), (:A {id: 2})-[:TYPE]->(:B {id: 2})
            """
        When executing query:
            """
            MATCH (a)-->(b), (c)-->(d), (e)-->(f) RETURN a, b, c, d, e, f
            """
        Then the result should be empty

    Scenario: Triple match with Cyphermorphism yields result
        Given an empty graph
        And having executed
            """
            CREATE (:A {id: 1})-[:TYPE]->(:B {id: 1}), (:A {id: 2})-[:TYPE]->(:B {id: 2}), (:A {id: 3})-[:TYPE]->(:B {id: 3})
            """
        When executing query:
            """
            MATCH (a)-->(b), (c)-->(d), (e)-->(f) RETURN a, b, c, d, e, f
            """
        Then the result should be:
            | a            | b            | c            | d            | e            | f            |
            | (:A {id: 1}) | (:B {id: 1}) | (:A {id: 2}) | (:B {id: 2}) | (:A {id: 3}) | (:B {id: 3}) |
            | (:A {id: 1}) | (:B {id: 1}) | (:A {id: 3}) | (:B {id: 3}) | (:A {id: 2}) | (:B {id: 2}) |
            | (:A {id: 2}) | (:B {id: 2}) | (:A {id: 1}) | (:B {id: 1}) | (:A {id: 3}) | (:B {id: 3}) |
            | (:A {id: 2}) | (:B {id: 2}) | (:A {id: 3}) | (:B {id: 3}) | (:A {id: 1}) | (:B {id: 1}) |
            | (:A {id: 3}) | (:B {id: 3}) | (:A {id: 2}) | (:B {id: 2}) | (:A {id: 1}) | (:B {id: 1}) |
            | (:A {id: 3}) | (:B {id: 3}) | (:A {id: 1}) | (:B {id: 1}) | (:A {id: 2}) | (:B {id: 2}) |

    Scenario: Same cyphermorphism group in 3 matches
        Given an empty graph
        And having executed
            """
            CREATE (:A)-[:TYPE]->(:B)-[:TYPE]->(:C)-[:TYPE]->(:D)-[:TYPE]->(:E)
            """
        When executing query:
            """
            MATCH (a:A)-->(b), (d)-->(e), (c)<--(b), (d)<--(c) RETURN a, b, c, d, e
            """
        Then the result should be:
            | a    | b    | c    | d    | e    |
            | (:A) | (:B) | (:C) | (:D) | (:E) |
