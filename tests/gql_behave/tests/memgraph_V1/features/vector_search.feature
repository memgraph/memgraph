Feature: Vector search related features

    Scenario: Create vector index
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type | label | property | count |
            | 'vector'   | 'L1'  | 'prop1'  | 0     |

    Scenario: Create vector index with all config options
        Given an empty graph
        And having executed
            """-
            CREATE VECTOR INDEX test_index ON :L1(prop1) WITH CONFIG {"dimension": 2, "capacity": 10, "metric": "cos", "resize_coefficient": 2, "scalar_kind": "i8"}
            """
        When executing query:
            """
            SHOW VECTOR INDEX INFO;
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind |
            | 64       | 2         | 'test_index' | 'L1'  | 'prop1'  | 'cos' | 0     | 'i8'        |

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
            | index type | label | property | count |
            | 'vector'   | 'L1'  | 'prop1'  | 1     |

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
            | index type | label | property | count |
            | 'vector'   | 'L1'  | 'prop1'  | 0     |

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
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind |
            | 64       | 2         | 'test_index' | 'L1'  | 'prop1'  | 'l2sq' | 0    | 'f32'       |

    Scenario: Get vector index info with cypher
        Given an empty graph
        And with new vector index test_index on :L1(prop1) with dimension 2 and capacity 10
        When executing query:
            """
            SHOW VECTOR INDEX INFO;
            """
        Then the result should be:
            | capacity | dimension | index_name   | label | property | metric | size | scalar_kind |
            | 64       | 2         | 'test_index' | 'L1'  | 'prop1'  | 'l2sq' | 0    | 'f32'       |

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
