Feature: Aggregations

    Scenario: Count test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a:A), (b:B), (c:C)
            """
        When executing query:
            """
            MATCH (a) RETURN COUNT(a) AS n
            """
        Then the result should be:
            | n |
            | 3 |

    Scenario: Count test 02:
        Given an empty graph
        When executing query:
            """
            RETURN COUNT(123) AS n
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Count test 03:
        Given an empty graph
        When executing query:
            """
            RETURN COUNT(true) AS n
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Count test 04:
        Given an empty graph
        When executing query:
            """
            RETURN COUNT('abcd') AS n
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Count test 05:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0}), (b{x: 0}), (c{x: 0}), (d{x: 1}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN COUNT(a) AS n, a.x
            """
        Then the result should be:
            | n | a.x |
            | 3 | 0   |
            | 2 | 1   |

    Scenario: Count test 06:
        Given an empty graph
        And having executed
            """
            CREATE (), (), (), (), ()
            """
        When executing query:
            """
            MATCH (n) RETURN COUNT(*) AS n
            """
        Then the result should be:
            | n |
            | 5 |

    Scenario: Count test 07:
        Given an empty graph
        When executing query:
        """
        RETURN count(null)
        """
        Then the result should be:
            | count(null) |
            | 0           |

    Scenario: Sum test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN SUM(a.x) AS n
            """
        Then an error should be raised

    Scenario: Sum test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b), (c{x: 5}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN SUM(a.x) AS n
            """
        Then the result should be:
            | n |
            | 6 |

    Scenario: Sum test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN SUM(a.y) AS n, a.x
            """
        Then the result should be:
            | n | a.x |
            | 4 | 0   |
            | 4 | 1   |

    Scenario: Sum test 04:
        Given an empty graph
        When executing query:
        """
        RETURN sum(null)
        """
        Then the result should be:
            | sum(null) |
            | 0         |

    Scenario: Avg test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN AVG(a.x) AS n
            """
        Then an error should be raised

    Scenario: Avg test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1.25}), (b), (c{x: 4.75}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN AVG(a.x) AS n
            """
        Then the result should be:
            | n   |
            | 3.0 |

    Scenario: Avg test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN AVG(a.y) AS n, a.x
            """
        Then the result should be:
            | n   | a.x |
            | 2.0 | 0   |
            | 4.0 | 1   |

    Scenario: Avg test 04:
        Given an empty graph
        When executing query:
        """
        RETURN avg(null)
        """
        Then the result should be:
            | avg(null) |
            | null      |

    Scenario: Min test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN MIN(a.x) AS n
            """
        Then an error should be raised

    Scenario: Min test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN MIN(a.x) AS n
            """
        Then the result should be:
            | n  |
            | 1  |

    Scenario: Min test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN MIN(a.y) AS n, a.x
            """
        Then the result should be:
            | n | a.x |
            | 1 | 0   |
            | 4 | 1   |

    Scenario: Min test 04:
        Given an empty graph
        When executing query:
        """
        RETURN min(null)
        """
        Then the result should be:
            | min(null) |
            | null      |

    Scenario: Min test 05: Date
        Given any graph
        When executing query:
            """
            UNWIND [date("2024-03-05"), date("2023-04-05"), date("2023-03-06")] AS i
            RETURN min(i)
            """
        Then the result should be:
            | min(i)     |
            | 2023-03-06 |
        And no side effects

    Scenario: Min test 06: LocalTime
        Given any graph
        When executing query:
            """
            UNWIND [localTime("09:16:00"), localTime("09:15:59"), localTime("10:15:00")] AS i
            RETURN min(i)
            """
        Then the result should be:
            | min(i)             |
            | 09:15:59.000000000 |
        And no side effects

    Scenario: Min test 07: LocalDateTime
        Given any graph
        When executing query:
            """
            UNWIND [localDateTime("2024-03-05T09:15:00"),
            localDateTime("2023-04-05T09:15:00"),
            localDateTime("2023-03-06T09:15:00"),
            localDateTime("2023-03-05T10:15:00"),
            localDateTime("2023-03-05T09:16:00"),
            localDateTime("2023-03-05T09:15:59")] AS i
            RETURN min(i)
            """
        Then the result should be:
            | min(i)                        |
            | 2023-03-05T09:15:59.000000000 |
        And no side effects

    Scenario: Min test 08: ZonedDateTime
        Given any graph
        When executing query:
            """
            UNWIND [datetime("2024-01-22T08:12:31[Etc/UTC]"),
            datetime("2024-01-22T08:42:31+00:30"),
            datetime("2024-01-22T08:57:31+00:45"),
            datetime("2024-01-22T09:12:31[Europe/Zurich]"),
            datetime("2024-01-22T09:12:31[Europe/Warsaw]")] AS i
            RETURN min(i)
            """
        Then the result should be:
            | min(i)                              |
            | 2024-01-22T08:12:31.000000000+00:00 |
        And no side effects

    Scenario: Min test 09: Duration
        Given any graph
        When executing query:
            """
            UNWIND [duration("PT2M2.33S"), duration("PT2M2.33S")] AS i
            RETURN min(i)
            """
        Then an error should be raised

    Scenario: Max test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
            """
        When executing query:
            """
            MATCH (a) RETURN MAX(a.x) AS n
            """
        Then an error should be raised

    Scenario: Max test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN MAX(a.x) AS n
            """
        Then the result should be:
            | n  |
            | 9  |

    Scenario: Max test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
            """
        When executing query:
            """
            MATCH (a) RETURN Max(a.y) AS n, a.x
            """
        Then the result should be:
            | n | a.x |
            | 3 | 0   |
            | 4 | 1   |

    Scenario: Max test 04:
        Given an empty graph
        When executing query:
        """
        RETURN max(null)
        """
        Then the result should be:
            | max(null) |
            | null      |

    Scenario: Max test 05: Date
        Given any graph
        When executing query:
            """
            UNWIND [date("2024-03-05"), date("2023-04-05"), date("2023-03-06")] AS i
            RETURN max(i)
            """
        Then the result should be:
            | max(i)     |
            | 2024-03-05 |
        And no side effects

    Scenario: Max test 06: LocalTime
        Given any graph
        When executing query:
            """
            UNWIND [localTime("10:15:00"), localTime("09:16:00"), localTime("09:15:59")] AS i
            RETURN max(i)
            """
        Then the result should be:
            | max(i)             |
            | 10:15:00.000000000 |
        And no side effects

    Scenario: Max test 07: LocalDateTime
        Given any graph
        When executing query:
            """
            UNWIND [localDateTime("2024-03-05T09:15:00"),
            localDateTime("2023-04-05T09:15:00"),
            localDateTime("2023-03-06T09:15:00"),
            localDateTime("2023-03-05T10:15:00"),
            localDateTime("2023-03-05T09:16:00"),
            localDateTime("2023-03-05T09:15:59")] AS i
            RETURN max(i)
            """
        Then the result should be:
            | max(i)                        |
            | 2024-03-05T09:15:00.000000000 |
        And no side effects

    Scenario: Max test 08: ZonedDateTime
        Given any graph
        When executing query:
            """
            UNWIND [datetime("2024-01-22T08:12:31[Etc/UTC]"),
            datetime("2024-01-22T08:42:31+00:30"),
            datetime("2024-01-22T08:57:31+00:45"),
            datetime("2024-01-22T09:12:31[Europe/Zurich]"),
            datetime("2024-01-22T09:12:31[Europe/Warsaw]")] AS i
            RETURN max(i)
            """
        Then the result should be:
            | max(i)                              |
            | 2024-01-22T09:12:31.000000000+01:00 |
        And no side effects

    Scenario: Max test 09: Duration
        Given any graph
        When executing query:
            """
            UNWIND [duration("PT2M2.33S"), duration("PT2M2.33S")] AS i
            RETURN max(i)
            """
        Then an error should be raised

    Scenario: Collect test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0}), (b{x: True}), (c{x: 'asdf'})
            """
        When executing query:
            """
            MATCH (a) RETURN collect(a.x) AS n
            """
        Then the result should be (ignoring element order for lists)
            | n                 |
            | [0, true, 'asdf'] |

    Scenario: Collect test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 0}), (b{x: True}), (c{x: 'asdf'}), (d{x: null})
            """
        When executing query:
            """
            MATCH (a) RETURN collect(a.x) AS n
            """
        Then the result should be (ignoring element order for lists)
            | n                 |
            | [0, true, 'asdf'] |

    Scenario: Collect test 03:
        Given an empty graph
        And having executed
            """
            CREATE ({k: "a", v: 3}), ({k: "b", v: 1}), ({k: "c", v: 2})
            """
        When executing query:
            """
            MATCH (a) RETURN collect(a.k + "_key", a.v + 10) AS n
            """
        Then the result should be
            | n                                 |
            | {a_key: 13, b_key: 11, c_key: 12} |

        Scenario: Combined aggregations - some evaluate to null:
        Given an empty graph
        And having executed
            """
            CREATE (f)
            CREATE (n {property: 1})
            """
        When executing query:
            """
            MATCH (n) RETURN count(n) < n.property, count(n.property), count(n), avg(n.property), min(n.property), max(n.property), sum(n.property)
            """
        Then the result should be:
            | count(n) < n.property | count(n.property)     | count(n)              | avg(n.property)       | min(n.property)       | max(n.property)       | sum(n.property)       |
            | false                 | 1                     | 1                     | 1.0                   | 1                     | 1                     | 1                     |
            | null                  | 0                     | 1                     | null                  | null                  | null                  | 0                     |

    Scenario: Graph projection test 01:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 2}), (c{x: 3}), (d{x: 4}), (a)-[:X]->(b), (b)-[:X]->(c), (c)-[:X]->(a), (a)-[:B]->(d)
            """
        When executing query:
            """
            MATCH p=()-[:X]->() WITH project(p) as graph WITH graph.nodes as nodes UNWIND nodes as n RETURN n.x as x ORDER BY x DESC
            """
        Then the result should be:
            | x |
            | 3 |
            | 2 |
            | 1 |

    Scenario: Graph projection test 02:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 2}), (c{x: 3}), (d{x: 4}), (a)-[:X]->(b), (b)-[:X]->(c), (c)-[:X]->(a), (a)-[:B]->(d)
            """
        When executing query:
            """
            MATCH p=()-[:Z]->() WITH project(p) as graph WITH graph.nodes as nodes UNWIND nodes as n RETURN n.x as x ORDER BY x DESC
            """
        Then the result should be:
            | x |

    Scenario: Graph projection test 03:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 2}), (c{x: 3}), (d{x: 4}), (a)-[:X {prop:1}]->(b), (b)-[:X {prop:2}]->(c), (c)-[:X {prop:3}]->(a), (a)-[:B {prop:4}]->(d)
            """
        When executing query:
            """
            MATCH p=()-[:X]->() WITH project(p) as graph WITH graph.edges as edges UNWIND edges as e RETURN e.prop as y ORDER BY y DESC
            """
        Then the result should be:
            | y |
            | 3 |
            | 2 |
            | 1 |

    Scenario: Graph projection test 04:
        Given an empty graph
        And having executed
            """
            CREATE (a{x: 1}), (b{x: 2}), (c{x: 3}), (d{x: 4}), (a)-[:X {prop:1}]->(b), (b)-[:X {prop:2}]->(c), (c)-[:X {prop:3}]->(a), (a)-[:B {prop:4}]->(d)
            """
        When executing query:
            """
            MATCH p=()-[:Z]->() WITH project(p) as graph WITH graph.edges as edges UNWIND edges as e RETURN e.prop as y ORDER BY y DESC
            """
        Then the result should be:
          | y |

    Scenario: Empty collect aggregation:
      Given an empty graph
      And having executed
          """
          CREATE (s:Subnet {ip: "192.168.0.1"})
          """
      When executing query:
          """
          MATCH (subnet:Subnet) WHERE FALSE WITH subnet, collect(subnet.ip) as ips RETURN id(subnet) as id
          """
      Then the result should be empty

    Scenario: Empty count aggregation
      Given an empty graph
      And having executed
          """
          CREATE (s:Subnet {ip: "192.168.0.1"})
          """
      When executing query:
          """
          MATCH (subnet:Subnet) WHERE FALSE WITH subnet, count(subnet.ip) as ips RETURN id(subnet) as id
          """
      Then the result should be empty

    Scenario: Collect nodes properties into a map:
      Given an empty graph
      And having executed
          """
          CREATE (t:Tag {short_code: "TST", description: "SYSTEM_TAG"}), (t2:Tag {short_code: "PRD", description: "SYSTEM_TAG"}),
          (t3:Tag {short_code: "STG", description: "SYSTEM_TAG"}), (device {name: "name1"}), (device)-[a1:ASSOCIATED]->(t),
          (device)-[a2:ASSOCIATED]->(t2), (device)-[a3:ASSOCIATED]->(t3);
          """
      When executing query:
          """
          MATCH (d {name: "name1"})-[t:ASSOCIATED]-(tag:Tag) RETURN collect({short_code: tag.short_code, description: tag.description}) as tags;
          """
      Then the result should be:
          | tags                                                                                                                                             |
          | [{description: 'SYSTEM_TAG', short_code: 'TST'}, {description: 'SYSTEM_TAG', short_code: 'PRD'}, {description: 'SYSTEM_TAG', short_code: 'STG'}] |

    Scenario: Count directly without WITH clause 01
      Given an empty graph
      And having executed
          """
          CREATE (:Node {prop1: 1, prop2: 2, prop3: 3}), (:Node {prop1: 10, prop2: 11, prop3: 12}), (:Node {prop1: 20, prop2: 21, prop3: 22})
          """
      When executing query:
      """
      MATCH (n) RETURN n.prop1, n.prop2, n.prop3, count(*) AS cnt
      """
      Then the result should be:
        | n.prop1 | n.prop2 | n.prop3 | cnt |
        | 20      | 21      | 22      | 1   |
        | 10      | 11      | 12      | 1   |
        | 1       | 2       | 3       | 1   |

    Scenario: Count directly without WITH clause 02
      Given an empty graph
      And having executed
          """
          CREATE (:Node {prop1: 1, prop2: 2, prop3: 3}), (:Node {prop1: 10, prop2: 11, prop3: 12}), (:Node {prop1: 20, prop2: 21, prop3: 22})
          """
      When executing query:
      """
      MATCH (n) WITH n.prop1 AS prop1, n.prop2 AS prop2, n.prop3 AS prop3 RETURN prop1, prop2, prop3, count(*) AS cnt;
      """
      Then the result should be:
        | prop1 | prop2 | prop3 | cnt |
        | 20    | 21    | 22    | 1   |
        | 10    | 11    | 12    | 1   |
        | 1     | 2     | 3     | 1   |
