Feature: Memgraph only tests (queries in which we choose to be incompatible with neo4j)

    Scenario: Multiple sets (undefined behaviour)
        Given an empty graph
        And having executed
            """
            CREATE (n{x: 3})-[:X]->(m{x: 5})
            """
        When executing query:
            """
            MATCH (n)--(m) SET n.x = n.x + 1 SET m.x = m.x + 2 SET m.x = n.x RETURN n.x
            """
	    # TODO: Figure out if we can define a test with multiple possible outputs in cucumber,
	    # until then this test just documents behaviour instead of testing it.
            #        Then the result should be:
            #            | n.x |    | n.x |
            #            |  5  | or |  7  |
            #            |  5  |    |  7  |

    Scenario: Multiple comparisons
        Given an empty graph
        When executing query:
            """
            RETURN 1 < 10 > 5 < 7 > 6 < 8 AS x
            """
        Then the result should be:
            | x    |
            | true |

    Scenario: Use deleted node
        Given an empty graph
        When executing query:
            """
            CREATE(a:A), (b:B), (c:C), (a)-[:T]->(b) WITH a DETACH DELETE a WITH a MATCH(a)-[r:T]->() RETURN r
            """
        Then an error should be raised

    Scenario: In test3
        When executing query:
            """
            WITH [[1], 2, 3, 4] AS l
            RETURN 1 IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: In test8
        When executing query:
            """
            WITH [[[[1]]], 2, 3, 4] AS l
            RETURN 1 IN l as x
            """
        Then the result should be:
            | x     |
            | false |

    Scenario: Keyword as symbolic name
        Given an empty graph
        And having executed
            """
            CREATE(a:DELete)
            """
        When executing query:
            """
            MATCH (n) RETURN n
            """
        Then the result should be:
            | n         |
            | (:DELete) |

    Scenario: Aggregation in CASE:
        Given an empty graph
        When executing query:
            """
            MATCH (n) RETURN CASE count(n) WHEN 10 THEN 10 END
            """
        Then an error should be raised

    Scenario: Create enum:
        Given an empty graph
        When executing query:
            """
            CREATE ENUM Status VALUES { Good, Bad };
            """
        Then the result should be empty

    Scenario: Show enums:
        Given an empty graph
        # Values will be used from the previous scenario
        When executing query:
            """
            SHOW ENUMS;
            """
        Then the result should be:
            | Enum Name | Enum Values     |
            | 'Status'  | ['Good', 'Bad'] |

    Scenario: Add value to enum:
        Given an empty graph
        And having executed
            """
            ALTER ENUM Status ADD VALUE Medium;
            """
        When executing query:
            """
            SHOW ENUMS;
            """
        Then the result should be:
            | Enum Name | Enum Values     |
            | 'Status'  | ['Good', 'Bad', 'Medium'] |

    Scenario: Update value in enum:
        Given an empty graph
        And having executed
            """
            ALTER ENUM Status UPDATE VALUE Medium TO Average;
            """
        When executing query:
            """
            SHOW ENUMS;
            """
        Then the result should be:
            | Enum Name | Enum Values     |
            | 'Status'  | ['Good', 'Bad', 'Average'] |

    Scenario: Compare enum values for equality:
        Given an empty graph
        # Values will be used from the previous scenario
        When executing query:
            """
            RETURN Status::Good = Status::Good AS result1, Status::Good = Status::Bad AS result2
            """
        Then the result should be:
            | result1 | result2 |
            | true    | false   |

    Scenario: Compare different enums for equality:
        Given an empty graph
        # Values will be used from the previous scenario
        And having executed
            """
            CREATE ENUM NewEnum VALUES { Good, Bad };
            """
        When executing query:
            """
            RETURN Status::Good = NewEnum::Good AS result1
            """
        Then the result should be:
            | result1 |
            | false  |

    Scenario: Create an edge with an enum property:
        Given an empty graph
        When executing query:
            """
            CREATE (n:Person {s: Status::Good})-[:KNOWS {s: Status::Bad}]->(m:Person {s: Status::Bad})
            """
        Then the result should be empty

    Scenario: Get nodes and edges with enum properties:
        Given an empty graph
        And having executed
            """
            CREATE (n:Person {s: Status::Good})-[:KNOWS {s: Status::Bad}]->(m:Person {s: Status::Bad})
            """
        When executing query:
            """
            MATCH (n)-[e]->(m) RETURN n, n.s, e, e.s, m
            """
        Then the result should be:
            | n                                                          | n.s                                       | e                                                        | e.s                                      | m                                                         |
            | (:Person{s:{'__type':'mg_enum','__value':'Status::Good'}}) | {__type:'mg_enum',__value:'Status::Good'} | [:KNOWS{s:{'__type':'mg_enum','__value':'Status::Bad'}}] | {__type:'mg_enum',__value:'Status::Bad'} | (:Person{s:{'__type':'mg_enum','__value':'Status::Bad'}}) |

    Scenario: Filter nodes by enum property equal op:
        Given an empty graph
        And having executed
            """
            CREATE (n:Person {s: Status::Good})-[:KNOWS {s: Status::Bad}]->(m:Person {s: Status::Bad})
            """
        When executing query:
            """
            MATCH (n) WHERE n.s = Status::Bad RETURN n
            """
        Then the result should be:
            | n                                                         |
            | (:Person{s:{'__type':'mg_enum','__value':'Status::Bad'}}) |

    Scenario: Filter nodes by enum property comparison op:
        Given an empty graph
        And having executed
            """
            CREATE (n:Person {s: Status::Good})-[:KNOWS {s: Status::Bad}]->(m:Person {s: Status::Bad})
            """
        When executing query:
            """
            MATCH (n) WHERE n.s <= Status::Bad RETURN n
            """
        Then an error should be raised

    Scenario: Compare enum values for inequality:
        Given an empty graph
        # Values will be used from the previous scenario
        When executing query:
            """
            RETURN Status::Good != Status::Good AS result1, Status::Good != Status::Bad AS result2
            """
        Then the result should be:
            | result1 | result2 |
            | false   | true    |

    Scenario: Alter enum remove value:
        Given an empty graph
        When executing query:
            """
            ALTER ENUM Status REMOVE VALUE Good;
            """
        Then an error should be raised

    Scenario: Drop enum:
        Given an empty graph
        When executing query:
            """
            DROP ENUM Status;
            """
        Then an error should be raised

    Scenario: Point creation default:
        When executing query:
            """
            RETURN
                point({x:null, y:2}).srid AS result1,
                point({x:1, y:2, z:3, k:null}).srid AS result2,
                point({x:1, y:2}).srid AS result3,
                point({x:1, y:2, z:3}).srid AS result4,
                point({longitude:1, latitude:2}).srid AS result5,
                point({longitude:1, latitude:2, height:3}).srid AS result6,
                point({longitude:1, latitude:2, z:3}).srid AS result7;
            """
        Then the result should be:
            | result1 | result2 | result3 | result4 | result5 | result6 | result7 |
            | null    | null    | 7203    | 9757    | 4326    | 4979    | 4979    |

    Scenario: Point creation srid:
        When executing query:
            """
            RETURN
                point({longitude:1, latitude:2, srid:7203}).srid AS result1,
                point({longitude:1, latitude:2, height:3, srid:9757}).srid AS result2,
                point({x:1, y:2, srid:4326}).srid AS result3,
                point({x:1, y:2, z:3, srid:4979}).srid AS result4;
            """
        Then the result should be:
            | result1 | result2 | result3 | result4 |
            | 7203    | 9757    | 4326    | 4979    |

    Scenario: Point creation crs:
        When executing query:
            """
            RETURN
                point({longitude:1, latitude:2, crs:'cartesian'}).srid AS result1,
                point({longitude:1, latitude:2, height:3, crs:'cartesian-3d'}).srid AS result2,
                point({x:1, y:2, crs:'wgs-84'}).srid AS result3,
                point({x:1, y:2, z:3, crs:'wgs-84-3d'}).srid AS result4;
            """
        Then the result should be:
            | result1 | result2 | result3 | result4 |
            | 7203    | 9757    | 4326    | 4979    |

    Scenario: Point creation failure 1:
        When executing query:
            """
            RETURN point({longitude:1, y:2}) AS result;
            """
        Then an error should be raised

    Scenario: Point creation failure 2:
        When executing query:
            """
            RETURN point({x:1, latitude:2}) AS result;
            """
        Then an error should be raised

    Scenario: Point creation failure 3:
        When executing query:
            """
            RETURN point({longitude:-191, latitude:0}) AS result;
            """
        Then an error should be raised

    Scenario: Point creation failure 4:
        When executing query:
            """
            RETURN point({longitude:0, latitude:91}) AS result;
            """
        Then an error should be raised

    Scenario: Point2d-WGS48 lookup:
        Given an empty graph
        When executing query:
            """
            CREATE (n:Location {coordinates: point({longitude: 1, latitude: 2})});
            """
        Then the result should be empty

        When executing query:
            """
            MATCH (n)
            RETURN
                n.coordinates.x as x_result,
                n.coordinates.longitude as longitude_result,
                n.coordinates.y as y_result,
                n.coordinates.latitude as latitude_result,
                n.coordinates.crs as crs_result,
                n.coordinates.srid as srid_result;
            """
        Then the result should be:
            | x_result | longitude_result | y_result | latitude_result | crs_result | srid_result |
            | 1.0      | 1.0              | 2.0      | 2.0             | 'wgs-84'   | 4326        |

    Scenario: Point2d-WGS48 lookup z:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.z as result;
            """
        Then an error should be raised

    Scenario: Point2d-WGS48 lookup height:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.height as result;
            """
        Then an error should be raised

    Scenario: Point3d-WGS48 lookup:
        Given an empty graph
        When executing query:
            """
            CREATE (n:Location {coordinates: point({longitude: 1, latitude: 2, height: 3})});
            """
        Then the result should be empty

        When executing query:
            """
            MATCH (n)
            RETURN
                n.coordinates.x as x_result,
                n.coordinates.longitude as longitude_result,
                n.coordinates.y as y_result,
                n.coordinates.latitude as latitude_result,
                n.coordinates.z as z_result,
                n.coordinates.height as height_result,
                n.coordinates.crs as crs_result,
                n.coordinates.srid as srid_result;
            """
        Then the result should be:
            | x_result | longitude_result | y_result | latitude_result | z_result | height_result | crs_result | srid_result |
            | 1.0      | 1.0              | 2.0      | 2.0             | 3.0      | 3.0           | 'wgs-84'   | 4979        |

    Scenario: Point2d-cartesian lookup:
        Given an empty graph
        When executing query:
            """
            CREATE (n:Location {coordinates: point({x: 1, y: 2})});
            """
        Then the result should be empty
        When executing query:
            """
            MATCH (n)
            RETURN
                n.coordinates.x as x_result,
                n.coordinates.y as y_result,
                n.coordinates.crs as crs_result,
                n.coordinates.srid as srid_result;
            """
        Then the result should be:
            | x_result | y_result | crs_result  | srid_result |
            | 1.0      | 2.0      | 'cartesian' | 7203        |

    Scenario: Point2d-cartesian lookup longitude:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.longitude as result;
            """
        Then an error should be raised

    Scenario: Point2d-cartesian lookup latitude:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.latitude as result;
            """
        Then an error should be raised

    Scenario: Point2d-cartesian lookup z:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.z as result;
            """
        Then an error should be raised

    Scenario: Point2d-cartesian lookup height:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.height as result;
            """
        Then an error should be raised

   Scenario: Point3d-cartesian lookup:
        Given an empty graph
        When executing query:
            """
            CREATE (n:Location {coordinates: point({x: 1, y: 2, z: 3})});
            """
        Then the result should be empty
        When executing query:
            """
            MATCH (n)
            RETURN
                n.coordinates.x as x_result,
                n.coordinates.y as y_result,
                n.coordinates.z as z_result,
                n.coordinates.crs as crs_result,
                n.coordinates.srid as srid_result;
            """
        Then the result should be:
            | x_result | y_result | z_result | crs_result  | srid_result |
            | 1.0      | 2.0      | 3.0      | 'cartesian' | 9757        |

    Scenario: Point3d-cartesian lookup longitude:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.longitude as result;
            """
        Then an error should be raised

    Scenario: Point3d-cartesian lookup latitude:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.latitude as result;
            """
        Then an error should be raised

    Scenario: Point3d-cartesian lookup height:
        When executing query:
            """
            MATCH (n) RETURN n.coordinates.height as result;
            """
        Then an error should be raised

    Scenario: Show schema info:
        Given an empty graph
        When executing query:
            """
            SHOW SCHEMA INFO;
            """
        Then an error should be raised
