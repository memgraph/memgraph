Feature: Vector cosine similarity related features

    Scenario: Cosine similarity of identical vectors
        Given an empty graph
        When executing query:
            """
            RETURN vector_search.cosine_similarity([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) AS similarity;
            """
        Then the result should be:
            | similarity |
            | 1.0        |

    Scenario: Cosine similarity of orthogonal vectors
        Given an empty graph
        When executing query:
            """
            RETURN vector_search.cosine_similarity([1.0, 0.0], [0.0, 1.0]) AS similarity;
            """
        Then the result should be:
            | similarity |
            | 0.0        |

    Scenario: Cosine similarity of opposite vectors
        Given an empty graph
        When executing query:
            """
            RETURN vector_search.cosine_similarity([1.0, 0.0], [-1.0, 0.0]) AS similarity;
            """
        Then the result should be:
            | similarity |
            | -1.0       |

    Scenario: Cosine similarity with mixed integer and float values
        Given an empty graph
        When executing query:
            """
            RETURN vector_search.cosine_similarity([1, 2.0, 3], [2.0, 4, 6.0]) AS similarity;
            """
        Then the result should be:
            | similarity |
            | 1.0        |

    Scenario: Cosine similarity raises error for empty vectors
        Given an empty graph
        When executing query:
            """
            RETURN vector_search.cosine_similarity([], []) AS similarity;
            """
        Then an error should be raised

    Scenario: Cosine similarity raises error for different dimension vectors
        Given an empty graph
        When executing query:
            """
            RETURN vector_search.cosine_similarity([1.0, 2.0], [1.0, 2.0, 3.0]) AS similarity;
            """
        Then an error should be raised

    Scenario: Cosine similarity raises error for zero vectors
        Given an empty graph
        When executing query:
            """
            RETURN vector_search.cosine_similarity([0.0, 0.0], [1.0, 2.0]) AS similarity;
            """
        Then an error should be raised
