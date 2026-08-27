Feature: Expressions

    Scenario: Test equal operator
        Given an empty graph
        When executing query:
            """
            CREATE (a)
            RETURN 1=1 and 1.0=1.0 and 'abc'='abc' and false=false and a.age is null as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test not equal operator
        Given an empty graph
        When executing query:
            """
            CREATE (a{age: 1})
            RETURN not 1<>1 and 1.0<>1.1 and 'abcd'<>'abc' and false<>true and a.age is not null as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test greater operator
        Given an empty graph
        When executing query:
            """
            RETURN 2>1 and not 1.0>1.1 and 'abcd'>'abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test less operator
        Given an empty graph
        When executing query:
            """
            RETURN not 2<1 and 1.0<1.1 and not 'abcd'<'abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test greater equal operator
        Given an empty graph
        When executing query:
            """
            RETURN 2>=2 and not 1.0>=1.1 and 'abcd'>='abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test less equal operator
        Given an empty graph
        When executing query:
            """
            RETURN 2<=2 and 1.0<=1.1 and not 'abcd'<='abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test plus operator
        Given an empty graph
        When executing query:
            """
            RETURN 3+2=1.09+3.91 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test minus operator
        Given an empty graph
        When executing query:
            """
            RETURN 3-2=1.09-0.09 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test multiply operator
        Given an empty graph
        When executing query:
            """
            RETURN 3*2=1.5*4 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test divide operator1
        Given an empty graph
        When executing query:
            """
            RETURN 3/2<>7.5/5 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test divide operator2
        Given an empty graph
        When executing query:
            """
            RETURN 3.0/2=7.5/5 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test mod operator
        Given an empty graph
        When executing query:
            """
            RETURN 3%2=1 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test one big logical equation
        Given an empty graph
        When executing query:
            """
            RETURN not true or true and false or not ((true xor false or true) and true or false xor true ) as n
            """
        Then the result should be:
            |   n   |
            | false |

    Scenario: Test IS NULL binds looser than arithmetic
        Given an empty graph
        When executing query:
            """
            RETURN (null + 1) * 1 IS NULL AS result
            """
        Then the result should be:
            | result |
            | true   |

    Scenario: Test NaN is unordered with every value
        Given an empty graph
        When executing query:
            """
            WITH 0.0 / 0.0 AS nan
            RETURN nan < 0 AS lt, nan <= 0 AS le, nan > 0 AS gt, nan >= 0 AS ge
            """
        Then the result should be:
            | lt    | le    | gt    | ge    |
            | false | false | false | false |

    Scenario: Test NaN is not greater than infinity
        Given an empty graph
        When executing query:
            """
            RETURN 0.0 / 0.0 > 1.0 / 0.0 AS result
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Test a NaN row does not pass a greater than filter
        Given an empty graph
        When executing query:
            """
            UNWIND [0.0 / 0.0, -1.0, 2.0] AS value
            WITH value
            WHERE value > 0
            RETURN value
            """
        Then the result should be:
            | value |
            | 2.0   |

    Scenario: Test list equality is Null when only a Null element separates the lists
        Given an empty graph
        When executing query:
            """
            RETURN [null] = [null] AS same, [null] = [1] AS different, [null] <> [null] AS negated
            """
        Then the result should be:
            | same | different | negated |
            | null | null      | null    |

    Scenario: Test map equality is Null when only a Null value separates the maps
        Given an empty graph
        When executing query:
            """
            RETURN {k: null} = {k: null} AS same, {k: null} = {k: 1} AS different
            """
        Then the result should be:
            | same | different |
            | null | null      |

    Scenario: Test a definitively unequal element settles list equality
        Given an empty graph
        When executing query:
            """
            RETURN [null, 1] = [null, 2] AS unequal, [null] = [null, null] AS shorter, [1, 2] = [1, 2] AS equal
            """
        Then the result should be:
            | unequal | shorter | equal |
            | false   | false   | true  |

    Scenario: Test WHERE drops rows whose list equality is Null
        Given an empty graph
        When executing query:
            """
            UNWIND [[null], [null], [1, 2]] AS a
            UNWIND [[null], [null], [1, 2]] AS b
            WITH a, b
            WHERE a = b
            RETURN count(*) AS result
            """
        Then the result should be:
            | result |
            | 1      |

    Scenario: Test DISTINCT still folds lists that hold a Null together
        Given an empty graph
        When executing query:
            """
            UNWIND [[null], [null], [1, null], [1, null]] AS value
            RETURN count(DISTINCT value) AS result
            """
        Then the result should be:
            | result |
            | 2      |

    Scenario: Test grouping still folds maps that hold a Null together
        Given an empty graph
        When executing query:
            """
            UNWIND [{k: null}, {k: null}, {k: 1}] AS value
            RETURN value, count(*) AS count
            ORDER BY count DESC
            """
        Then the result should be:
            | value       | count |
            | {k: null}   | 2     |
            | {k: 1}      | 1     |

    Scenario: Test IN is Null for a Null value whatever else the rows hold
        Given an empty graph
        When executing query:
            """
            UNWIND [10, 0, null] AS v
            RETURN v, v IN [10] AS result
            """
        Then the result should be:
            | v    | result |
            | 10   | true   |
            | 0    | false  |
            | null | null   |

    Scenario: Test WHERE drops a row whose negated IN is Null
        Given an empty graph
        When executing query:
            """
            UNWIND [10, 0, null] AS v
            WITH v
            WHERE NOT (v IN [10])
            RETURN v
            """
        Then the result should be:
            | v |
            | 0 |

    Scenario: Test IN over stored properties drops the rows it cannot decide
        Given an empty graph
        And having executed:
            """
            CREATE (:N {id: 1, v: 10}), (:N {id: 2, v: 0}), (:N {id: 3, v: null}), (:N {id: 4, v: -5}), (:N {id: 5})
            """
        When executing query:
            """
            MATCH (n:N)
            WHERE NOT (n.v IN [10])
            RETURN n.id AS id
            ORDER BY id
            """
        Then the result should be:
            | id |
            | 2  |
            | 4  |

    Scenario: Test IN is Null when only a Null element could match
        Given an empty graph
        When executing query:
            """
            RETURN [null] IN [[null]] AS nested, 1 IN [1, null] AS found, 2 IN [1, null] AS absent
            """
        Then the result should be:
            | nested | found | absent |
            | null   | true  | null   |

    Scenario: Test IN over an empty list is false even for a Null value
        Given an empty graph
        When executing query:
            """
            UNWIND [1, null] AS v
            RETURN v, v IN [] AS result
            """
        Then the result should be:
            | v    | result |
            | 1    | false  |
            | null | false  |

    Scenario: Test a join on list equality drops the pairs it cannot decide
        Given an empty graph
        And having executed:
            """
            CREATE (:A {p: [null]}), (:A {p: [null]}), (:A {p: [1, 2]})
            """
        When executing query:
            """
            MATCH (a:A), (b:A)
            WHERE a.p = b.p
            RETURN count(*) AS result
            """
        Then the result should be:
            | result |
            | 1      |

    Scenario: Test booleans are ordered with false before true
        Given an empty graph
        When executing query:
            """
            RETURN false < true AS lt, true > false AS gt, false <= false AS le, true >= true AS ge
            """
        Then the result should be:
            | lt   | gt   | le   | ge   |
            | true | true | true | true |

    Scenario: Test a boolean is ordered against nothing else
        Given an empty graph
        When executing query:
            """
            RETURN false < 1 AS number, false < 'a' AS string, false < null AS nothing
            """
        Then the result should be:
            | number | string | nothing |
            | null   | null   | null    |

    Scenario: Test lists are ordered element by element
        Given an empty graph
        When executing query:
            """
            RETURN [1, 2] < [1, 3] AS later, [[1]] < [[2]] AS nested, [1, 2] <= [1, 2] AS same
            """
        Then the result should be:
            | later | nested | same |
            | true  | true   | true |

    Scenario: Test a shorter list comes first whatever would have followed
        Given an empty graph
        When executing query:
            """
            RETURN [1] < [1, 0] AS number, [1] < [1, null] AS nothing
            """
        Then the result should be:
            | number | nothing |
            | true   | true    |

    Scenario: Test a list is undecided when a compared element is
        Given an empty graph
        When executing query:
            """
            RETURN [1, 2] >= [1, null] AS unknown, [1, null] < [2, null] AS settled_first, [1] < ['a'] AS unlike
            """
        Then the result should be:
            | unknown | settled_first | unlike |
            | null    | true          | null   |

    Scenario: Test a list holding NaN has no place in the ordering
        Given an empty graph
        When executing query:
            """
            RETURN [0.0 / 0.0] < [1.0] AS lt, [0.0 / 0.0] > [1.0] AS gt
            """
        Then the result should be:
            | lt   | gt   |
            | null | null |
