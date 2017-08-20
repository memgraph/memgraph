Feature: String operators

    Scenario: StartsWith test1
        Given an empty graph
        And having executed
            """
            CREATE(a{name: "ai'M'e"}), (b{name: "AiMe"}), (c{name: "aime"})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name STARTS WITH 'aim'
            return n.name
            """
        Then the result should be:
            | n.name |
            | 'aime' |

    Scenario: StartsWith test2
        Given an empty graph
        And having executed
            """
            CREATE(a{name: "ai'M'e"}), (b{name: "AiMe"}), (c{name: "aime"})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name STARTS WITH "ai'M"
            return n.name
            """
        Then the result should be:
            | n.name   |
            | 'ai'M'e' |

    Scenario: StartsWith test3
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name STARTS WITH 1
            return n.name
            """
        Then an error should be raised

    Scenario: StartsWith test5
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name STARTS WITH true
            return n.name
            """
        Then an error should be raised


    Scenario: EndsWith test1
        Given an empty graph
        And having executed
            """
            CREATE(a{name: "ai'M'E"}), (b{name: "AiMe"}), (c{name: "aime"})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name ENDS WITH 'e'
            return n.name
            """
        Then the result should be:
            | n.name |
            | 'AiMe' |
            | 'aime' |

    Scenario: EndsWith test2
        Given an empty graph
        And having executed
            """
            CREATE(a{name: "ai'M'e"}), (b{name: "AiMe"}), (c{name: "aime"})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name ENDS WITH "M'e"
            return n.name
            """
        Then the result should be:
            | n.name   |
            | 'ai'M'e' |

    Scenario: EndsWith test3
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name ENDS WITH 1
            return n.name
            """
        Then an error should be raised

    Scenario: EndsWith test5
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name ENDS WITH true
            return n.name
            """
        Then an error should be raised


    Scenario: Contains test1
        Given an empty graph
        And having executed
            """
            CREATE(a{name: "ai'M'e"}), (b{name: "AiMe"}), (c{name: "aime"})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name CONTAINS 'iM'
            return n.name
            """
        Then the result should be:
            | n.name |
            | 'AiMe' |

    Scenario: Contains test2
        Given an empty graph
        And having executed
            """
            CREATE(a{name: "ai'M'e"}), (b{name: "AiMe"}), (c{name: "aime"})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name CONTAINS "i'M"
            return n.name
            """
        Then the result should be:
            | n.name   |
            | 'ai'M'e' |

    Scenario: Contains test3
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name CONTAINS 1
            return n.name
            """
        Then an error should be raised


    Scenario: Contains test5
        Given an empty graph
        And having executed
            """
            CREATE(a{name: 1}), (b{name: 2}), (c{name: null})
            """
        When executing query:
            """
            MATCH (n)
            WHERE n.name CONTAINS true
            return n.name
            """
        Then an error should be raised
