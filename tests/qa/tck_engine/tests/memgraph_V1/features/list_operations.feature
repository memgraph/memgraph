Feature: List operators

    Scenario: In test1
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN 3 IN l as in
            """
        Then the result should be:
            | in   |
            | true |

    Scenario: In test2
        When executing query:
            """
            WITH [1, '2', 3, 4] AS l
            RETURN 2 IN l as in
            """
        Then the result should be:
            | in    |
            | false |
 
    Scenario: In test4
        When executing query:
            """
            WITH [1, [2, 3], 4] AS l
            RETURN [3, 2] IN l as in
            """
        Then the result should be:
            | in    |
            | false |

    Scenario: In test5
        When executing query:
            """
            WITH [[1, 2], 3, 4] AS l
            RETURN 1 IN l as in
            """
        Then the result should be:
            | in    |
            | false |

    Scenario: In test6
        When executing query:
            """
            WITH [1, [[2, 3], 4]] AS l
            RETURN [[2, 3], 4] IN l as in
            """
        Then the result should be:
            | in   |
            | true |

    Scenario: In test7
        When executing query:
            """
            WITH [1, [[2, 3], 4]] AS l
            RETURN [1, [[2, 3], 4]] IN l as in
            """
        Then the result should be:
            | in    |
            | false |

    

    Scenario: Index test1
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[2] as in
            """
        Then the result should be:
            | in |
            | 3  |

    Scenario: Index test2
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-2] as in
            """
        Then the result should be:
            | in |
            | 3  |

    Scenario: Index test3
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[2][0] as in
            """
        Then an error should be raised

    Scenario: Index test4
        When executing query:
            """
            WITH [1, 2, [3], 4] AS l
            RETURN l[2][0] as in
            """
        Then the result should be:
            | in |
            | 3  |

    Scenario: Index test5
        When executing query:
            """
            WITH [[1, [2, [3]]], 4] AS l
            RETURN l[0][1][1][0] as in
            """
        Then the result should be:
            | in |
            | 3  |


    Scenario: Slice test1
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[0..2] as in
            """
        Then the result should be, in order:
            | in     |
            | [1, 2] |

    Scenario: Slice test2
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-2..5] as in
            """
        Then the result should be, in order:
            | in     |
            | [3, 4] |

    Scenario: Slice test3
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-2..4] as in
            """
        Then the result should be, in order:
            | in     |
            | [3, 4] |

    Scenario: Slice test4
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-1..4] as in
            """
        Then the result should be, in order:
            | in  |
            | [4] |

    Scenario: Slice test5
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-2..-2] as in
            """
        Then the result should be, in order:
            | in |
            | [] |

    Scenario: Slice test6
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[4..-2] as in
            """
        Then the result should be, in order:
            | in |
            | [] |


    Scenario: Concatenate test1
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l1, [5, 6, 7] AS l2
            RETURN l1+l2 as in
            """
        Then the result should be, in order:
            | in                    |
            | [1, 2, 3, 4, 5, 6, 7] |

    Scenario: Concatenate test2
        When executing query:
            """
            WITH [[1, [2]]] AS l1, [[[3], 4]] AS l2
            RETURN l1+l2 as in
            """
        Then the result should be, in order:
            | in                   |
            | [[1, [2]], [[3], 4]] |
    
    Scenario: Concatenate test3
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l1, NULL AS l2
            RETURN l1+l2 as in
            """
        Then the result should be, in order:
            | in   |
            | null |
    
    Scenario: Concatenate test4
        When executing query:
            """
            WITH [] AS l1, [] AS l2
            RETURN l1+l2 as in
            """
        Then the result should be, in order:
            | in |
            | [] |

    Scenario: Unwind test
        When executing query:
            """
            UNWIND [ [[1], 2], [3], 4] as l
            RETURN l
            """
        Then the result should be:
            | l        |
            | [[1], 2] |
            | [3]      |
            | 4        |
