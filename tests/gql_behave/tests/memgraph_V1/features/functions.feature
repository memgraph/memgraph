Feature: Functions

    Scenario: Sqrt test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN SQRT(a.x) AS n
            """
        Then an error should be raised

    Scenario: Sqrt test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN SQRT(a.x) AS n
            """
        Then the result should be:
            | n    |
            | 1.0  |
            | null |
            | 3.0  |
            | null |

    Scenario: ToBoolean test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 0}))
            """
        When executing query:
            """
            MATCH (a) RETURN TOBOOLEAN(a.x) AS n
            """
        Then an error should be raised

    Scenario: ToBoolean test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 'TrUe'}), (b{x: 'not bool'}), (c{x: faLsE}), (d{x: null}), (e{x: 'fALse'}), (f{x: tRuE})
            """
        When executing query:
            """
            MATCH (a) RETURN TOBOOLEAN(a.x) AS n
            """
        Then the result should be:
            | n     |
            | true  |
            | null  |
            | false |
            | null  |
            | false |
            | true  |

    Scenario: ToInteger test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN TOINTEGER(a.x) AS n
            """
        Then an error should be raised

    Scenario: ToInteger test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 'not int'}), (c{x: '-12'}), (d{x: null}), (e{x: '1.2'}), (f{x: '1.9'})
            """
        When executing query:
            """
            MATCH (a) RETURN TOINTEGER(a.x) AS n
            """
        Then the result should be:
            | n    |
            | 1    |
            | null |
            | -12  |
            | null |
            | 1    |
            | 1    |


    Scenario: ToFloat test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN TOFLOAT(a.x) AS n
            """
        Then an error should be raised

    Scenario: ToFloat test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 'not float'}), (c{x: '-12'}), (d{x: null}), (e{x: '1.2'}), (f{x: 1.9})
            """
        When executing query:
            """
            MATCH (a) RETURN TOFLOAT(a.x) AS n
            """
        Then the result should be:
            | n     |
            | 1.0   |
            | null  |
            | -12.0 |
            | null  |
            | 1.2   |
            | 1.9   |


    Scenario: Abs test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true})
            """
        When executing query:
            """
            MATCH (a) RETURN ABS(a.x) AS n
            """
        Then an error should be raised

    Scenario: Abs test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})
            """
        When executing query:
            """
            MATCH (a) RETURN ABS(a.x) AS n
            """
        Then an error should be raised

    Scenario: Abs test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (c{x: -12}), (d{x: null}), (e{x: -2.3}), (f{x: 1.9})
            """
        When executing query:
            """
            MATCH (a) RETURN ABS(a.x) AS n
            """
        Then the result should be:
            | n    |
            | 1    |
            | 12   |
            | null |
            | 2.3  |
            | 1.9  |


    Scenario: Exp test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN EXP(a.x) AS n
            """
        Then an error should be raised

    Scenario: Exp test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN EXP(a.x) AS n
            """
        Then an error should be raised

    Scenario: Exp test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (c{x: -12}), (d{x: null}), (e{x: -2.3}), (f{x: 1.9})
            """
        When executing query:
            """
            MATCH (a) RETURN EXP(a.x) AS n
            """
        Then the result should be:
            | n                     |
            | 2.718281828459045     |
            | .00000614421235332821 |
            | null                  |
            | 0.10025884372280375   |
            | 6.6858944422792685    |

    Scenario: Log test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN LOG(a.x) AS n
            """
        Then an error should be raised

    Scenario: Log test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN LOG(a.x) AS n
            """
        Then an error should be raised

    Scenario: Log test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
            """
        When executing query:
            """
            MATCH (a) RETURN LOG(a.x) AS n
            """
        Then the result should be:
            | n                   |
            | -2.0955709236097197 |
            | nan                 |
            | null                |
            | 3.295836866004329   |

    Scenario: Log10 test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN LOG10(a.x) AS n
            """
        Then an error should be raised

    Scenario: Log10 test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN LOG10(a.x) AS n
            """
        Then an error should be raised

    Scenario: Log10 test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
            """
        When executing query:
            """
            MATCH (a) RETURN LOG10(a.x) AS n
            """
        Then the result should be:
            | n                   |
            | -0.9100948885606021 |
            | nan                 |
            | null                |
            | 1.4313637641589874  |


    Scenario: Sin test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN SIN(a.x) AS n
            """
        Then an error should be raised

    Scenario: Sin test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN SIN(a.x) AS n
            """
        Then an error should be raised

    Scenario: Sin test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
            """
        When executing query:
            """
            MATCH (a) RETURN SIN(a.x) AS n
            """
        Then the result should be:
            | n                   |
            | 0.12269009002431533 |
            | 0.5365729180004349  |
            | null                |
            | 0.956375928404503   |

    Scenario: Cos test 01:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: true}))
            """
        When executing query:
            """
            MATCH (a) RETURN COS(a.x) AS n
            """
        Then an error should be raised

    Scenario: Cos test 02:
        Given an empty graph
        And having executed
            """
            CREATE (b{x: '1.0'})),
            """
        When executing query:
            """
            MATCH (a) RETURN COS(a.x) AS n
            """
        Then an error should be raised

    Scenario: Cos test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
            """
        When executing query:
            """
            MATCH (a) RETURN COS(a.x) AS n
            """
        Then the result should be:
            | n                   |
            | 0.9924450321351935  |
            | 0.8438539587324921  |
            | null                |
            | -0.2921388087338362 |

    Scenario: Sign test:
        When executing query:
            """
            RETURN SIGN(null) AS n, SIGN(123) AS a, SIGN(0) AS b, SIGN(1.23) AS c,
            SIGN(-123) AS d, SIGN(-1.23) AS e, SIGN(-0.00) AS f
            """
        Then the result should be:
            | n      | a | b | c | d  |  e | f |
            | null   | 1 | 0 | 1 | -1 | -1 | 0 |

    Scenario: Tan test:
        When executing query:
            """
            RETURN TAN(null) AS n, TAN(123) AS a, TAN(-2.5) AS b, TAN(1.23) AS c
            """
        Then the result should be:
            | n      | a                  | b                  | c                 |
            | null   | 0.5179274715856552 | 0.7470222972386603 | 2.819815734268152 |

    Scenario: Atan test:
        When executing query:
            """
            RETURN ATAN(null) AS n, ATAN(1.23) AS a, ATAN(123) AS b, ATAN(0) AS c
            """
        Then the result should be:
            | n      | a                  | b                  | c   |
            | null   | 0.8881737743776796 | 1.5626664246149526 | 0.0 |

    Scenario: Atan2 test:
        When executing query:
            """
            RETURN ATAN2(1, null) AS n, ATAN2(0, 0) AS a, ATAN2(2, 3) AS b, ATAN2(1.5, 2.5) AS c
            """
        Then the result should be:
            | n      | a   | b                  | c                  |
            | null   | 0.0 | 0.5880026035475675 | 0.5404195002705842 |

    Scenario: Asin test:
        When executing query:
            """
            RETURN ASIN(null) AS n, ASIN(1.23) AS a, ASIN(0.48) AS b, ASIN(-0.25) AS c
            """
        Then the result should be:
            | n      | a   | b                  | c                    |
            | null   | nan | 0.5006547124045881 | -0.25268025514207865 |

    Scenario: Acos test:
        When executing query:
            """
            RETURN ACOS(null) AS n, ACOS(1.23) AS a, ACOS(0.48) AS b, ACOS(-0.25) AS c
            """
        Then the result should be:
            | n      | a   | b                  | c                  |
            | null   | nan | 1.0701416143903084 | 1.8234765819369754 |

    Scenario: Round test:
        When executing query:
            """
            RETURN ROUND(null) AS n, ROUND(1.49999) AS a, ROUND(-1.5) AS b, ROUND(-1.51) AS c,
            ROUND(1.5) as d
            """
        Then the result should be:
            | n      | a   | b    | c    | d   |
            | null   | 1.0 | -2.0 | -2.0 | 2.0 |

    Scenario: Floor test:
        When executing query:
            """
            RETURN FLOOR(null) AS n, FLOOR(1.49999) AS a, FLOOR(-1.5) AS b, FLOOR(-1.51) AS c,
            FLOOR(1.00) as d
            """
        Then the result should be:
            | n      | a   | b    | c    | d   |
            | null   | 1.0 | -2.0 | -2.0 | 1.0 |

    Scenario: Ceil test:
        When executing query:
            """
            RETURN CEIL(null) AS n, CEIL(1.49999) AS a, CEIL(-1.5) AS b, CEIL(-1.51) AS c,
            CEIL(1.00) as d
            """
        Then the result should be:
            | n      | a   | b    | c    | d   |
            | null   | 2.0 | -1.0 | -1.0 | 1.0 |

    Scenario: Tail test:
        When executing query:
            """
            RETURN TAIL(null) AS n, TAIL([[1, 2], 3, 4]) AS a, TAIL([1, [2, 3, 4]]) AS b
            """
        Then the result should be:
            | n      | a      | b           |
            | null   | [3, 4] | [[2, 3, 4]] |

    Scenario: Range test:
        When executing query:
            """
            RETURN RANGE(1, 3) AS a, RANGE(1, 5, 2) AS b, RANGE(1, -1) AS c,
            RANGE(1, -1, -3) as d
            """
        Then the result should be:
            | a         | b         | c  | d   |
            | [1, 2, 3] | [1, 3, 5] | [] | [1] |

    Scenario: Size test:
        When executing query:
            """
            RETURN SIZE(null) AS n, SIZE([[1, 2], 3, 4]) AS a, SIZE([1, [2, 3, 4]]) AS b
            """
        Then the result should be:
            | n    | a | b |
            | null | 3 | 2 |

    Scenario: Degree test:
        When executing query:
            """
            CREATE (a)-[:Type]->(b)<-[:Type]-(c)
            RETURN DEGREE(a) AS da, DEGREE(b) AS db, DEGREE(null) AS dn
            """
        Then the result should be:
            | da | db | dn   |
            | 1  | 2  | null |


    Scenario: Last test:
        When executing query:
            """
            RETURN LAST(null) AS n, LAST([[1, 2], 3, 4]) AS a, LAST([1, [2, 3, 4]]) AS b
            """
        Then the result should be:
            | n    | a | b         |
            | null | 4 | [2, 3, 4] |

    Scenario: Head test:
        When executing query:
            """
            RETURN HEAD(null) AS n, HEAD([[1, 2], 3, 4]) AS a, HEAD([1, [2, 3, 4]]) AS b
            """
        Then the result should be:
            | n    | a      | b |
            | null | [1, 2] | 1 |

    Scenario: Nodes test:
        Given an empty graph
        And having executed:
            """
            CREATE (:L1)-[:E1]->(:L2)-[:E2]->(:L3)
            """
        When executing query:
            """
            MATCH p=()-[]->()-[]->() RETURN nodes(p) AS ns
            """
        Then the result should be:
            |           ns          |
            | [(:L1), (:L2), (:L3)] |

    Scenario: Relationships test:
        Given an empty graph
        And having executed:
            """
            CREATE (:L1)-[:E1]->(:L2)-[:E2]->(:L3)
            """
        When executing query:
            """
            MATCH p=()-[]->()-[]->() RETURN relationships(p) as rels
            """
        Then the result should be:
            |      rels      |
            | [[:E1], [:E2]] |

    Scenario: Labels test:
        Given an empty graph
        And having executed:
            """
            CREATE(:x:y:z), (), (:a:b)
            """
        When executing query:
            """
            MATCH(n) RETURN LABELS(n) AS l
            """
        Then the result should be:
            | l               |
            | []              |
            | ['x', 'y', 'z'] |
            | ['a', 'b']      |

    Scenario: Type test:
        Given an empty graph
        And having executed:
            """
            CREATE(a), (b), (c), (a)-[:A]->(b), (a)-[:B]->(c), (b)-[:C]->(c)
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN TYPE(r) AS t
            """
        Then the result should be:
            | t   |
            | 'A' |
            | 'B' |
            | 'C' |

    Scenario: Properties test1:
        Given an empty graph
        And having executed:
            """
            CREATE(a), (b), (c), (a)-[:A{a: null}]->(b), (a)-[:B{b: true}]->(c),
            (b)-[:C{c: 123}]->(c)
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN PROPERTIES(r) AS p ORDER BY p.prop;
            """
        Then the result should be:
            | p         |
            | {}        |
            | {b: true} |
            | {c: 123}  |

    Scenario: Properties test2:
        Given an empty graph
        And having executed:
            """
            CREATE({a: 'x'}), ({n: 1.1}), ()
            """
        When executing query:
            """
            MATCH(n) RETURN PROPERTIES(n) AS p
            """
        Then the result should be:
            | p        |
            | {}       |
            | {a: 'x'} |
            | {n: 1.1} |

    Scenario: Coalesce test:
        When executing query:
            """
            RETURN COALESCE(null) AS n, COALESCE([null, null]) AS a,
            COALESCE(null, null, 1, 2, 3) AS b
            """
        Then the result should be:
            | n    | a            | b |
            | null | [null, null] | 1 |

    Scenario: Endnode test:
        Given an empty graph
        And having executed:
            """
            CREATE(a:x), (b:y), (c:z), (a)-[:A]->(b), (b)-[:B]->(c), (c)-[:C]->(b)
            """
        When executing query:
            """
            MATCH ()-[r]->() RETURN ENDNODE(r) AS n, ENDNODE(r):z AS a, ENDNODE(r):y AS b
            """
        Then the result should be:
            | n    | a     | b     |
            | (:z) | true  | false |
            | (:y) | false | true  |
            | (:y) | false | true  |

    Scenario: E test:
        When executing query:
            """
            RETURN E() as n
            """
        Then the result should be:
            | n                   |
            | 2.718281828459045   |

    Scenario: Pi test:
        When executing query:
            """
            RETURN PI() as n
            """
        Then the result should be:
            | n                   |
            | 3.141592653589793   |

    Scenario: Rand test:
        When executing query:
            """
            WITH rand() as r RETURN r >= 0.0 AND r < 1.0 as result
            """
        Then the result should be:
            | result |
            | true   |

    Scenario: All test 01:
        When executing query:
            """
            RETURN all(x IN [1, 2, '3'] WHERE x < 2) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: All test 02:
        When executing query:
            """
            RETURN all(x IN [1, 2, 3] WHERE x < 4) AS a
            """
        Then the result should be:
            | a     |
            | true |

    Scenario: All test 03:
        When executing query:
            """
            RETURN all(x IN [1, 2, '3'] WHERE x < 3) AS a
            """
        Then an error should be raised

    Scenario: All test 04:
        When executing query:
            """
            RETURN all(x IN [Null, Null, Null] WHERE x = 0) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: All test 05:
        When executing query:
            """
            RETURN all(x IN [Null, Null, 0] WHERE x = 0) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: All test 06:
        When executing query:
            """
            RETURN all(x IN [Null, Null, 0] WHERE x > 0) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: All test 07:
        When executing query:
            """
            RETURN all(x IN [Null, Null, Null] WHERE x = Null) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: All test 08:
        When executing query:
            """
            RETURN all(x IN ["a", "b", "c"] WHERE x = Null) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: Single test 01:
        When executing query:
            """
            RETURN single(x IN [1, 2, '3'] WHERE x < 4) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: Single test 02:
        When executing query:
            """
            RETURN single(x IN [1, 2, 3] WHERE x = 1) AS a
            """
        Then the result should be:
            | a    |
            | true |

    Scenario: Single test 03:
        When executing query:
            """
            RETURN single(x IN [1, 2, '3'] WHERE x > 2) AS a
            """
        Then an error should be raised

    Scenario: Single test 04:
        When executing query:
            """
            RETURN single(x IN [Null, Null, Null] WHERE x = 0) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: Single test 05:
        When executing query:
            """
            RETURN single(x IN [Null, Null, 0] WHERE x = 0) AS a
            """
        Then the result should be:
            | a    |
            | true |

    Scenario: Single test 06:
        When executing query:
            """
            RETURN single(x IN [Null, 0, Null, 0] WHERE x = 0) AS a
            """
        Then the result should be:
            | a    |
            | false |

    Scenario: Single test 07:
        When executing query:
            """
            RETURN single(x IN [Null, Null, 0] WHERE x > 0) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: Single test 08:
        When executing query:
            """
            RETURN single(x IN [Null, 1, Null, 0] WHERE x > 0) AS a
            """
        Then the result should be:
            | a     |
            | true  |

    Scenario: Single test 09:
        When executing query:
            """
            RETURN single(x IN [Null, Null, Null] WHERE x = Null) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: Single test 10:
        When executing query:
            """
            RETURN single(x IN ["a", "b", "c"] WHERE x = Null) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: Any test 01:
        When executing query:
            """
            RETURN any(x IN [1, 2, 3] WHERE x > 0) AS a
            """
        Then the result should be:
            | a    |
            | true |

    Scenario: Any test 02:
        When executing query:
            """
            RETURN any(x IN [1, 2, 3] WHERE x = 0) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: Any test 03:
        When executing query:
            """
            RETURN any(x IN ["a", "b", "c"] WHERE x = 1) AS a
            """
        Then the result should be:
            | a     |
            | false |


    Scenario: Any test 04:
        When executing query:
            """
            RETURN any(x IN [Null, Null, Null] WHERE x = 0) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: Any test 05:
        When executing query:
            """
            RETURN any(x IN [Null, Null, 0] WHERE x = 0) AS a
            """
        Then the result should be:
            | a    |
            | true |

   Scenario: Any test 06:
        When executing query:
            """
            RETURN any(x IN [Null, Null, 0] WHERE x > 0) AS a
            """
        Then the result should be:
            | a     |
            | false |

   Scenario: Any test 07:
        When executing query:
            """
            RETURN any(x IN [Null, Null, Null] WHERE x = Null) AS a
            """
        Then the result should be:
            | a    |
            | null |

   Scenario: Any test 08:
        When executing query:
            """
            RETURN any(x IN ["a", "b", "c"] WHERE x = Null) AS a
            """
        Then the result should be:
            | a    |
            | null |

   Scenario: None test 01:
        When executing query:
            """
            RETURN none(x IN [1, 2, 3] WHERE x < 1) AS a
            """
        Then the result should be:
            | a    |
            | true |

    Scenario: None test 02:
        When executing query:
            """
            RETURN none(x IN [1, 2, 3] WHERE x = 1) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: None test 03:
        When executing query:
            """
            RETURN none(x IN ["a", "b", "c"] WHERE x = 1) AS a
            """
        Then the result should be:
            | a    |
            | true |

    Scenario: None test 04:
        When executing query:
            """
            RETURN none(x IN [Null, Null, Null] WHERE x = 0) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: None test 05:
        When executing query:
            """
            RETURN none(x IN [Null, Null, 0] WHERE x > 0) AS a
            """
        Then the result should be:
            | a    |
            | true |

    Scenario: None test 06:
        When executing query:
            """
            RETURN none(x IN [Null, Null, 0] WHERE x = 0) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: None test 07:
        When executing query:
            """
            RETURN none(x IN [Null, Null, Null] WHERE x = Null) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: None test 08:
        When executing query:
            """
            RETURN none(x IN ["a", "b", "c"] WHERE x = Null) AS a
            """
        Then the result should be:
            | a    |
            | null |

    Scenario: PredicateFunctions test 01:
        When executing query:
            """
            WITH [1, 2, 3] as lst
            RETURN ALL(x IN lst WHERE x > 0) as all,
            SINGLE(x IN lst WHERE x > 0) AS single,
            ANY(x IN lst WHERE x > 0) AS any,
            NONE(x IN lst WHERE x > 0) AS none
            """
        Then the result should be:
            | all  | single | any  | none  |
            | true | false  | true | false |

    Scenario: PredicateFunctions test 02:
        When executing query:
            """
            WITH [1, 2, 3] as lst
            RETURN ALL(x IN lst WHERE x > 1) as all,
            SINGLE(x IN lst WHERE x > 1) AS single,
            ANY(x IN lst WHERE x > 1) AS any,
            NONE(x IN lst WHERE x > 1) AS none
            """
        Then the result should be:
            | all   | single | any  | none  |
            | false | false  | true | false |

    Scenario: PredicateFunctions test 03:
        When executing query:
            """
            WITH [1, 2, 3] as lst
            RETURN ALL(x IN lst WHERE x > 2) as all,
            SINGLE(x IN lst WHERE x > 2) AS single,
            ANY(x IN lst WHERE x > 2) AS any,
            NONE(x IN lst WHERE x > 2) AS none
            """
        Then the result should be:
            | all   | single | any  | none  |
            | false | true   | true | false |

    Scenario: PredicateFunctions test 04:
        When executing query:
            """
            WITH [1, 2, 3] as lst
            RETURN ALL(x IN lst WHERE x > 3) as all,
            SINGLE(x IN lst WHERE x > 3) AS single,
            ANY(x IN lst WHERE x > 3) AS any,
            NONE(x IN lst WHERE x > 3) AS none
            """
        Then the result should be:
            | all   | single | any   | none  |
            | false | false  | false | true  |

    Scenario: Reduce test 01:
        When executing query:
            """
            RETURN reduce(a = true, x IN [1, 2, '3'] | a AND x < 2) AS a
            """
        Then the result should be:
            | a     |
            | false |

    Scenario: Reduce test 02:
        When executing query:
            """
            RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS s
            """
        Then the result should be:
            | s |
            | 6 |

    Scenario: Reduce test 03:
        When executing query:
            """
            RETURN reduce(a = true, x IN [true, true, '3'] | a AND x) AS a
            """
        Then an error should be raised

    Scenario: Assert test fail, no message:
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1})
            """
        When executing query:
            """
            MATCH (n) RETURN assert(n.a = 2) AS res
            """
        Then an error should be raised


    Scenario: Assert test fail:
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1, b: "string"})
            """
        When executing query:
            """
            MATCH (n) RETURN assert(n.a = 2, n.b) AS res
            """
        Then an error should be raised


    Scenario: Assert test pass:
        Given an empty graph
        And having executed:
            """
            CREATE ({a: 1, b: "string"})
            """
        When executing query:
            """
            MATCH (n) RETURN assert(n.a = 1, n.b) AS res
            """
        Then the result should be:
            | res  |
            | true |

    Scenario: Counter test:
        Given an empty graph
        And having executed:
            """
            CREATE (), (), ()
            """
        When executing query:
            """
            MATCH (n) SET n.id = counter("n.id", 0) WITH n SKIP 1
            RETURN n.id, counter("other", 0) AS c2
            """
        Then the result should be:
            | n.id | c2 |
            | 1    | 0  |
            | 2    | 1  |

    Scenario: Vertex Id test:
        Given an empty graph
        And having executed:
            """
            CREATE (), (), ()
            """
        When executing query:
            """
            MATCH (n) WITH n ORDER BY id(n)
            WITH COLLECT(id(n)) AS node_ids
            UNWIND node_ids AS node_id
            RETURN node_id - node_ids[0] AS id;
            """
        Then the result should be:
            | id |
            | 0  |
            | 1  |
            | 2  |

    Scenario: Edge Id test:
        Given an empty graph
        And having executed:
            """
            CREATE (v1)-[e1:A]->(v2)-[e2:A]->(v3)-[e3:A]->(v4)
            """
        When executing query:
            """
            MATCH ()-[e]->() WITH e ORDER BY id(e)
            WITH COLLECT(id(e)) AS edge_ids
            UNWIND edge_ids AS edge_id
            RETURN edge_id - edge_ids[0] AS id;
            """
        Then the result should be:
            | id |
            | 0  |
            | 1  |
            | 2  |

    Scenario: Aggregate distinct does not impact other aggregates:
        Given an empty graph
        And having executed:
            """
            CREATE (:Node_A {id:1})
            CREATE (:Node_A {id:2})
            CREATE (:Node_A {id:3})
            CREATE (:Node_B {id:1})
            CREATE (:Node_B {id:2})
            CREATE (:Node_B {id:3})
            CREATE (:Node_B {id:4})
            CREATE (:Node_B {id:4})
            """
        When executing query:
            """
            MATCH (a:Node_A), (b:Node_B)
            RETURN COUNT(DISTINCT a.id) AS A_COUNT,
                   COUNT(b.id) AS B_COUNT;
            """
        Then the result should be:
            | A_COUNT | B_COUNT |
            | 3       | 15      |

    Scenario: Exists is forbidden within reduce:
      Given an empty graph
      And having executed:
        """
        CREATE ()
        """
      When executing query:
        """
        MATCH () WHERE reduce(a=exists(()),b in []|a) RETURN 1;
        """
      Then an error should be raised
