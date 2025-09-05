Feature: Vector search related features

    Scenario: Create vector index
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                | label | property | count |
            | 'label+property_vector'   | 'L1'  | 'prop1'  | 0     |

    Scenario: Create vector index with all config options
        Given an empty graph
        And having executed
            """
            CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10, "metric": "cos", "resize_coefficient": 2, "scalar_kind": "i8"}
            """
        When executing query:
            """
            SHOW VECTOR INDEX INFO;
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind | index_type              |
            | 64       | 2         | 'test_index' | 'L1'  | 'prop1'  | 'cos'  | 0    | 'i8'        | 'label+property_vector' |

    Scenario: Add node to vector index
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (:L1 {prop1: [1.0, 2.0]});
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                | label | property | count |
            | 'label+property_vector'   | 'L1'  | 'prop1'  | 1     |

    Scenario: Remove node from vector index
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
            """
            CREATE (:L1 {prop1: [1.0, 2.0]});
            """
        And having executed
            """
            MATCH (n) DELETE n
            """
        And having executed:
            """
            FREE MEMORY
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                | label | property | count |
            | 'label+property_vector'   | 'L1'  | 'prop1'  | 0     |

    Scenario: Drop vector index
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            DROP VECTOR INDEX test_index
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type             | label | property | count |

    Scenario: Get vector index info
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        When executing query:
            """
            CALL vector_search.show_index_info() YIELD * RETURN *;
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind | index_type              |
            | 64       | 2         | 'test_index' | 'L1'  | 'prop1'  | 'l2sq' | 0    | 'f32'       | 'label+property_vector' |

    Scenario: Get vector index info with cypher
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        When executing query:
            """
            SHOW VECTOR INDEX INFO;
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind | index_type              |
            | 64       | 2         | 'test_index' | 'L1'  | 'prop1'  | 'l2sq' | 0    | 'f32'       | 'label+property_vector' |

    Scenario: Search vector index
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (:L1 {prop1: [1.0, 2.0]})
            CREATE (:L1 {prop1: [1.0, 1.0]})
            CREATE (:L1 {prop1: [100.0, 150.0]})
            """
        When executing query:
            """
            CALL vector_search.search("test_index", 2, [1.0, 1.0]) YIELD * RETURN *;
            """
        Then the result should be:
            | distance   | node                      | similarity |
            | 0.0        | (:L1 {prop1: [1.0, 1.0]}) | 1.0        |
            | 1.0        | (:L1 {prop1: [1.0, 2.0]}) | 0.5        |

    Scenario: Vector search performs on float values
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (:L1 {prop1: [1.1, 1.1]})
            CREATE (:L1 {prop1: [2.1, 2.1]})
            """
        When executing query:
            """
            CALL vector_search.search("test_index", 1, [1.0, 1.0]) YIELD * RETURN node;
            """
        Then the result should be:
            | node                      |
            | (:L1 {prop1: [1.1, 1.1]}) |

    Scenario: Vector search performs on integer values
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (:L1 {prop1: [1.1, 1.1]})
            CREATE (:L1 {prop1: [2.1, 2.1]})
            """
        When executing query:
            """
            CALL vector_search.search("test_index", 1, [2, 2]) YIELD * RETURN node;
            """
        Then the result should be:
            | node                      |
            | (:L1 {prop1: [2.1, 2.1]}) |

    Scenario: Vector search raises error on value that is not integer or double
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        And having executed
            """
            CREATE (:L1 {prop1: [1.1, 1.1]})
            CREATE (:L1 {prop1: [2.1, 2.1]})
            """
        When executing query:
            """
            CALL vector_search.search("test_index", 1, ["invalid", "invalid"]) YIELD * RETURN node;
            """
        Then an error should be raised


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

    Scenario: Cosine similarity of identical vectors
        Given an empty graph
        When executing query:
            """
            CALL vector_search.cosine_similarity([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) YIELD similarity RETURN similarity;
            """
        Then the result should be:
            | similarity |
            | 1.0        |

    Scenario: Cosine similarity of orthogonal vectors
        Given an empty graph
        When executing query:
            """
            CALL vector_search.cosine_similarity([1.0, 0.0], [0.0, 1.0]) YIELD similarity RETURN similarity;
            """
        Then the result should be:
            | similarity |
            | 0.0        |

    Scenario: Cosine similarity of opposite vectors
        Given an empty graph
        When executing query:
            """
            CALL vector_search.cosine_similarity([1.0, 0.0], [-1.0, 0.0]) YIELD similarity RETURN similarity;
            """
        Then the result should be:
            | similarity |
            | -1.0       |

    Scenario: Cosine similarity with mixed integer and float values
        Given an empty graph
        When executing query:
            """
            CALL vector_search.cosine_similarity([1, 2.0, 3], [2.0, 4, 6.0]) YIELD similarity RETURN similarity;
            """
        Then the result should be:
            | similarity |
            | 1.0        |

    Scenario: Cosine similarity raises error for empty vectors
        Given an empty graph
        When executing query:
            """
            CALL vector_search.cosine_similarity([], []) YIELD similarity RETURN similarity;
            """
        Then an error should be raised

    Scenario: Cosine similarity raises error for different dimension vectors
        Given an empty graph
        When executing query:
            """
            CALL vector_search.cosine_similarity([1.0, 2.0], [1.0, 2.0, 3.0]) YIELD similarity RETURN similarity;
            """
        Then an error should be raised

    Scenario: Cosine similarity raises error for zero vectors
        Given an empty graph
        When executing query:
            """
            CALL vector_search.cosine_similarity([0.0, 0.0], [1.0, 2.0]) YIELD similarity RETURN similarity;
            """
        Then an error should be raised
