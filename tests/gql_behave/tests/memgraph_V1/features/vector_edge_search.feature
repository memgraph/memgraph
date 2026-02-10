Feature: Vector edge search related features

    Scenario: Create vector edge index
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        When executing query:
            """
            SHOW VECTOR INDEX INFO
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind | index_type                  |
            | 64       | 2         | 'test_index' | 'E1'  | 'prop1'  | 'l2sq' | 0    | 'f32'       | 'edge-type+property_vector' |

    Scenario: Add edge to vector edge index
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (a)-[:E1 {prop1: [1.0, 2.0]}]->(b);
            """
        When executing query:
            """
            SHOW VECTOR INDEX INFO
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind | index_type                  |
            | 64       | 2         | 'test_index' | 'E1'  | 'prop1'  | 'l2sq' | 1    | 'f32'       | 'edge-type+property_vector' |

    Scenario: Remove edge from vector edge index
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (a)-[:E1 {prop1: [1.0, 2.0]}]->(b);
            """
        And having executed
            """
            MATCH (a)-[e:E1]->(b) DELETE e
            """
        And having executed:
            """
            FREE MEMORY
            """
        When executing query:
            """
            SHOW VECTOR INDEX INFO
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind | index_type                  |
            | 64       | 2         | 'test_index' | 'E1'  | 'prop1'  | 'l2sq' | 0    | 'f32'       | 'edge-type+property_vector' |

    Scenario: Drop vector edge index
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            DROP VECTOR INDEX test_index
            """
        When executing query:
            """
            SHOW VECTOR INDEX INFO
            """
        Then the result should be:
            | index type | label | property | count |

    Scenario: Get vector edge index info with query module
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        When executing query:
            """
            CALL vector_search.show_index_info() YIELD * RETURN *;
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind | index_type                  |
            | 64       | 2         | 'test_index' | 'E1'  | 'prop1'  | 'l2sq' | 0    | 'f32'       | 'edge-type+property_vector' |

    Scenario: Search vector edge index
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (a)-[:E1 {prop1: [1.0, 2.0]}]->(b)
            CREATE (a)-[:E1 {prop1: [1.0, 1.0]}]->(b)
            CREATE (a)-[:E1 {prop1: [100.0, 150.0]}]->(b)
            """
        When executing query:
            """
            CALL vector_search.search_edges("test_index", 2, [1.0, 1.0]) YIELD * RETURN *;
            """
        Then the result should be:
            | distance   | edge                      |  similarity |
            | 0.0        | [:E1 {prop1: [1.0, 1.0]}] | 1.0         |
            | 1.0        | [:E1 {prop1: [1.0, 2.0]}] | 0.5         |

    Scenario: Vector edge search performs on float values
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (a)-[:E1 {prop1: [1.1, 1.1]}]->(b)
            CREATE (a)-[:E1 {prop1: [2.1, 2.1]}]->(b)
            """
        When executing query:
            """
            CALL vector_search.search_edges("test_index", 1, [1.0, 1.0]) YIELD * RETURN edge;
            """
        Then the result should be:
            | edge                      |
            | [:E1 {prop1: [1.1, 1.1]}] |

    Scenario: Vector edge search performs on integer values
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (a)-[:E1 {prop1: [1.1, 1.1]}]->(b)
            CREATE (a)-[:E1 {prop1: [2.1, 2.1]}]->(b)
            """
        When executing query:
            """
            CALL vector_search.search_edges("test_index", 1, [2, 2]) YIELD * RETURN edge;
            """
        Then the result should be:
            | edge                      |
            | [:E1 {prop1: [2.1, 2.1]}] |

    Scenario: Vector edge search raises error on value that is not integer or double
        Given an empty graph
        And with new vector edge index test_index on :E1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (a)-[:E1 {prop1: [1.1, 1.1]}]->(b)
            CREATE (a)-[:E1 {prop1: [2.1, 2.1]}]->(b)
            """
        When executing query:
            """
            CALL vector_search.search_edges("test_index", 1, ["invalid", "invalid"]) YIELD * RETURN edge;
            """
        Then an error should be raised
