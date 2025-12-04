Feature: Caching

    Scenario: List cache invalidation 1
        Given an empty graph
        And having executed:
            """
            CREATE ({value:1})
            CREATE ({value:2})
            """
        When executing query:
            """
            MATCH (n) WHERE n.value IN [n.value] RETURN n ORDER BY n.value ASC;
            """
        Then the result should be:
            |   n            |
            |  ({value: 1})  |
            |  ({value: 2})  |

    Scenario: List cache invalidation 2 (expression)
        Given an empty graph
        And having executed:
            """
            CREATE ({value:1})
            CREATE ({value:2})
            """
        When executing query:
            """
            MATCH (n) WHERE (n.value + 1) IN [n.value + 1] RETURN n ORDER BY n.value ASC;
            """
        Then the result should be:
            |   n            |
            |  ({value: 1})  |
            |  ({value: 2})  |

    Scenario: List cache invalidation 3
        Given an empty graph
        And having executed:
            """
            CREATE ({value:1, list:[2]})
            CREATE ({value:2, list:[3]})
            CREATE ({value:3, list:[]})
            """
        When executing query:
            """
            WITH [1] AS lst
            MATCH (n)
            WHERE n.value IN lst
            WITH n.list AS lst, n
            MATCH (m)
            WHERE m.value IN lst
            RETURN n, m;
            """
        Then the result should be:
            | n                       | m                       |
            |({list: [2], value: 1}) | ({list: [3], value: 2}) |

    Scenario: IN LIST with frame-dependent list elements
        Given an empty graph
        And having executed:
            """
            CREATE ()-[:ET1]->()
            CREATE ()-[:ET2]->()
            """
        When executing query:
            """
            MATCH ()-[e1]->()
            MATCH ()-[e2]->(), ()-[e3]->()
            WHERE e2 IN [e1, e3]
            RETURN type(e1) AS e1_type, type(e2) AS e2_type, type(e3) AS e3_type
            ORDER BY e1_type, e3_type
            """
        Then the result should be, in order:
            | e1_type | e2_type | e3_type |
            | 'ET1'   | 'ET1'   | 'ET2'   |
            | 'ET2'   | 'ET2'   | 'ET1'   |

    Scenario: Regex cache invalidation 1
        Given an empty graph
        And having executed:
            """
            CREATE ({name:'test1'})
            CREATE ({name:'test2'})
            """
        When executing query:
            """
            MATCH (n) WHERE n.name =~ n.name RETURN n ORDER BY n.name ASC;
            """
        Then the result should be:
            |   n              |
            |  ({name: 'test1'})  |
            |  ({name: 'test2'})  |

    Scenario: Regex cache invalidation 2
        Given an empty graph
        And having executed:
            """
            CREATE ({name:'test1', pattern:'.*est2'})
            CREATE ({name:'test2', pattern:'.*est3'})
            CREATE ({name:'test3', pattern:'.*est4'})
            """
        When executing query:
            """
            WITH '.*est1' AS regex_pattern
            MATCH (n)
            WHERE n.name =~ regex_pattern
            WITH n.pattern AS regex_pattern, n
            MATCH (m)
            WHERE m.name =~ regex_pattern
            RETURN n, m;
            """
        Then the result should be:
            | n                          | m                          |
            | ({name: 'test1', pattern: '.*est2'}) | ({name: 'test2', pattern: '.*est3'}) |

    Scenario: REGEX MATCH with frame-dependent regex elements
        Given an empty graph
        And having executed:
            """
            CREATE ({name:'test1'})
            CREATE ({name:'test2'})
            """
        When executing query:
            """
            MATCH (n1)
            MATCH (n2), (n3)
            WHERE (n2.name =~ n1.name OR n2.name =~ n3.name)
            AND n2 != n3
            RETURN n1.name AS n1_name, n2.name AS n2_name, n3.name AS n3_name
            ORDER BY n1_name, n2_name, n3_name
            """
        Then the result should be, in order:
            | n1_name | n2_name | n3_name |
            | 'test1' | 'test1' | 'test2' |
            | 'test2' | 'test2' | 'test1' |
