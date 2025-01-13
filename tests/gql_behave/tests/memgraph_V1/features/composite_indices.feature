Feature: Composite indices
    Scenario: One node retrieval:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node {prop1: 1, prop2: 1})
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 1     | 1     |

    Scenario: First prop less than and second prop equal:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node) WHERE n.prop1 < 2 and n.prop2 = 1
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 1     | 1     |

    Scenario: First prop less than or equal and second prop equal:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node) WHERE n.prop1 <= 2 and n.prop2 = 1
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 1     | 1     |
            | 2     | 1     |

    Scenario: First prop greater than and second prop equal:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node) WHERE n.prop1 > 9 and n.prop2 = 1
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 10    | 1     |


    Scenario: First prop greater than or equal and second prop equal:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node) WHERE n.prop1 > 9 and n.prop2 = 1
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 9     | 1     |
            | 10    | 1     |

    Scenario: First prop less than and second prop less than:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node) WHERE n.prop1 < 2 and n.prop2 < 2
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 1     | 1     |

    Scenario: First prop less than or equal and second prop less than:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node) WHERE n.prop1 <= 2 and n.prop2 < 2
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 1     | 1     |
            | 2     | 1     |

    Scenario: First prop less than and second prop less than or equal:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node) WHERE n.prop1 < 2 and n.prop2 <= 2
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 1     | 1     |
            | 1     | 2     |

    Scenario: First prop less than or equal and second prop less than or equal:
        Given graph "composite_indices_graph"
        And with new index :Node(prop1, prop2)
        When executing query:
            """
            MATCH (n:Node) WHERE n.prop1 <= 2 and n.prop2 <= 2
            RETURN n.prop1 AS prop1, n.prop2 AS prop2;
            """
        Then the result should be:
            | prop1 | prop2 |
            | 1     | 1     |
            | 1     | 2     |
            | 2     | 1     |
            | 2     | 2     |
