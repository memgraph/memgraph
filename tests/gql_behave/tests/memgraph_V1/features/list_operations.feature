Feature: List operators

    Scenario: In test1
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN 3 IN l as x
            """
        Then the result should be:
            | x    |
            | true |

    Scenario: In test2
        When executing query:
            """
            WITH [1, '2', 3, 4] AS l
            RETURN 2 IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: In test4
        When executing query:
            """
            WITH [1, [2, 3], 4] AS l
            RETURN [3, 2] IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: In test5
        When executing query:
            """
            WITH [[1, 2], 3, 4] AS l
            RETURN 1 IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: In test6
        When executing query:
            """
            WITH [1, [[2, 3], 4]] AS l
            RETURN [[2, 3], 4] IN l as x
            """
        Then the result should be:
            | x    |
            | true |

    Scenario: In test7
        When executing query:
            """
            WITH [1, [[2, 3], 4]] AS l
            RETURN [1, [[2, 3], 4]] IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: Index test1
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[2] as x
            """
        Then the result should be:
            | x |
            | 3 |

    Scenario: Index test2
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-2] as x
            """
        Then the result should be:
            | x |
            | 3 |

    Scenario: Index test3
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[2][0] as x
            """
        Then an error should be raised

    Scenario: Index test4
        When executing query:
            """
            WITH [1, 2, [3], 4] AS l
            RETURN l[2][0] as x
            """
        Then the result should be:
            | x |
            | 3 |

    Scenario: Index test5
        When executing query:
            """
            WITH [[1, [2, [3]]], 4] AS l
            RETURN l[0][1][1][0] as x
            """
        Then the result should be:
            | x |
            | 3 |

    Scenario: Slice test1
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[0..2] as x
            """
        Then the result should be, in order:
            | x      |
            | [1, 2] |

    Scenario: Slice test2
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-2..5] as x
            """
        Then the result should be, in order:
            | x      |
            | [3, 4] |

    Scenario: Slice test3
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-2..4] as x
            """
        Then the result should be, in order:
            | x      |
            | [3, 4] |

    Scenario: Slice test4
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-1..4] as x
            """
        Then the result should be, in order:
            | x   |
            | [4] |

    Scenario: Slice test5
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[-2..-2] as x
            """
        Then the result should be, in order:
            | x  |
            | [] |

    Scenario: Slice test6
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l
            RETURN l[4..-2] as x
            """
        Then the result should be, in order:
            | x  |
            | [] |

    Scenario: Concatenate test1
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l1, [5, 6, 7] AS l2
            RETURN l1+l2 as x
            """
        Then the result should be, in order:
            | x                     |
            | [1, 2, 3, 4, 5, 6, 7] |

    Scenario: Concatenate test2
        When executing query:
            """
            WITH [[1, [2]]] AS l1, [[[3], 4]] AS l2
            RETURN l1+l2 as x
            """
        Then the result should be, in order:
            | x                    |
            | [[1, [2]], [[3], 4]] |

    Scenario: Concatenate test3
        When executing query:
            """
            WITH [1, 2, 3, 4] AS l1, NULL AS l2
            RETURN l1+l2 as x
            """
        Then the result should be, in order:
            | x    |
            | null |

    Scenario: Concatenate test4
        When executing query:
            """
            WITH [] AS l1, [] AS l2
            RETURN l1+l2 as x
            """
        Then the result should be, in order:
            | x  |
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

    Scenario: Unwind + InList test1
        When executing query:
            """
            UNWIND [[1,2], [3,4]] as l
            RETURN 2 in l as x
            """
        Then the result should be:
            | x     |
            | true  |
            | false |

    Scenario: Unwind + InList test2
        When executing query:
            """
            WITH [[1,2], [3,4]] as list
            UNWIND list as l
            RETURN 2 in l as x
            """
        Then the result should be:
            | x     |
            | true  |
            | false |

     Scenario: Unwind + InList test3
        Given an empty graph
        And having executed
            """
            CREATE ({id: 1}), ({id: 2}), ({id: 3}), ({id: 4})
            """
        When executing query:
            """
            WITH [1, 2, 3] as list
            MATCH (n) WHERE n.id in list
            WITH n
            WITH n, [1, 2] as list
            WHERE n.id in list
            RETURN n.id as id
            ORDER BY id;
            """
        Then the result should be:
            | id |
            | 1  |
            | 2  |

     Scenario: InList 01
        Given an empty graph
        And having executed
            """
            CREATE (o:Node) SET o.Status = 'This is the status';
            """
        When executing query:
            """
            match (o:Node)
            where o.Status IN ['This is not the status', 'This is the status']
            return o;
            """
        Then the result should be:
            | o                                       |
            | (:Node {Status: 'This is the status'})  |

     Scenario: Simple list pattern comprehension
        Given graph "graph_keanu"
        When executing query:
            """
            MATCH (keanu:Person {name: 'Keanu Reeves'})
            RETURN [(keanu)-->(b:Movie) WHERE b.title CONTAINS 'Matrix' | b.released] AS years
            """
        Then the result should be:
            | years                    |
            | [2003, 2003, 1999, 2021] |

    Scenario: List pattern comprehension and property
        Given graph "graph_keanu"
        When executing query:
            """
            MATCH (keanu:Person {name: 'Keanu Reeves'})
            RETURN [(keanu)-->(b:Movie) WHERE b.title CONTAINS 'Matrix' | b.released] AS years, keanu.name
            """
        Then the result should be:
            | years                    | keanu.name     |
            | [2003, 2003, 1999, 2021] | 'Keanu Reeves' |

    Scenario: List pattern comprehension with function on selected property
        Given graph "graph_keanu"
        When executing query:
            """
            MATCH (keanu:Person {name: 'Keanu Reeves'})
            RETURN [(keanu)-->(b:Movie) WHERE b.title CONTAINS 'Matrix' | size(b.title)] AS movie_lens;
            """
        Then the result should be:
            | movie_lens       |
            | [22, 19, 10, 24] |

     Scenario: Multiple entries with list pattern comprehension
        Given graph "graph_keanu"
        When executing query:
            """
            MATCH (n:Person)
            RETURN n.name, [(n)-->(b:Movie) WHERE b.title CONTAINS 'Matrix' | b.released] AS years
            """
        Then the result should be:
            | n.name               | years                    |
            | 'Keanu Reeves'       | [2003, 2003, 1999, 2021] |
            | 'Carrie-Anne Moss'   | [1999, 2003]             |
            | 'Laurence Fishburne' | [1999]                   |

     Scenario: Multiple list pattern comprehensions in Return
        Given graph "graph_keanu"
        When executing query:
            """
            MATCH (n:Person)
            RETURN n.name,
                [(n)-->(b:Movie) WHERE b.title CONTAINS 'Matrix' | b.released] AS years,
                [(n)-->(c:Movie) WHERE c.title CONTAINS 'Matrix' | c.title] AS titles
            """
        Then the result should be:
            | n.name               | years                 | titles                                                                            |
            | 'Keanu Reeves'       | [2003,2003,1999,2021] | ['TheMatrixRevolutions','TheMatrixReloaded','TheMatrix','TheMatrixResurrections'] |
            | 'Carrie-Anne Moss'   | [1999,2003]           | ['TheMatrix','TheMatrixReloaded']                                                 |
            | 'Laurence Fishburne' | [1999]                | ['The Matrix']                                                                    |
     Scenario: Multiple list pattern comprehensions with the same symbol name inside in Return
        Given graph "graph_keanu"
        When executing query:
            """
            MATCH (n:Person)
            RETURN n.name,
                [(n)-->(b:Movie) WHERE b.title CONTAINS 'Matrix' | b.released] AS years,
                [(n)-->(b:Movie) WHERE b.title CONTAINS 'Matrix' | b.title] AS titles
            """
        Then the result should be:
            | n.name               | years                 | titles                                                                            |
            | 'Keanu Reeves'       | [2003,2003,1999,2021] | ['TheMatrixRevolutions','TheMatrixReloaded','TheMatrix','TheMatrixResurrections'] |
            | 'Carrie-Anne Moss'   | [1999,2003]           | ['TheMatrix','TheMatrixReloaded']                                                 |
            | 'Laurence Fishburne' | [1999]                | ['The Matrix']                                                                    |

     Scenario: Function inside pattern comprehension's expression
        Given graph "graph_keanu"
        When executing query:
            """
            MATCH (keanu:Person {name: 'Keanu Reeves'})
            RETURN [p = (keanu)-->(b:Movie) WHERE b.title CONTAINS 'Matrix' | size(nodes(p))] AS nodes
            """
        Then an error should be raised

     Scenario: Multiple list pattern comprehensions in With
        Given graph "graph_keanu"
        When executing query:
            """
            MATCH (n) WHERE size(n.name) > 5
            WITH
                n AS actor,
                [(n)-->(m) WHERE m.released > 2000 | m.title] AS titles,
                [(n)-->(m) WHERE m.released > 2000 | m.released] AS years
            RETURN actor.name, years, titles;
            """
        Then an error should be raised
