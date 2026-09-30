Feature: Spatial related features

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
            | null    | null    | 7203    | 9157    | 4326    | 4979    | 4979    |

    Scenario: Point creation srid:
        When executing query:
            """
            RETURN
                point({x:1, y:2, srid:7203}).srid AS result1,
                point({x:1, y:2, height:3, srid:9157}).srid AS result2,
                point({x:1, y:2, srid:4326}).srid AS result3,
                point({x:1, y:2, z:3, srid:4979}).srid AS result4;
            """
        Then the result should be:
            | result1 | result2 | result3 | result4 |
            | 7203    | 9157    | 4326    | 4979    |

    Scenario: Point creation crs:
        When executing query:
            """
            RETURN
                point({x:1, y:2, crs:'cartesian'}).srid AS result1,
                point({x:1, y:2, height:3, crs:'cartesian-3d'}).srid AS result2,
                point({x:1, y:2, crs:'wgs-84'}).srid AS result3,
                point({x:1, y:2, z:3, crs:'wgs-84-3d'}).srid AS result4;
            """
        Then the result should be:
            | result1 | result2 | result3 | result4 |
            | 7203    | 9157    | 4326    | 4979    |

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
            RETURN point({longitude:191, latitude:0}) AS result;
            """
        Then an error should be raised

    Scenario: Point creation failure 5:
        When executing query:
            """
            RETURN point({longitude:0, latitude:91}) AS result;
            """
        Then an error should be raised

    Scenario: Point creation failure 6:
        When executing query:
            """
            RETURN point({longitude:0, latitude:-91}) AS result;
            """
        Then an error should be raised

    Scenario: Point creation failure 7:
        When executing query:
            """
            RETURN point({longitude:1, latitude:2, crs:'cartesian'}) as result;
            """
        Then an error should be raised

    Scenario: Point creation failure 8:
        When executing query:
            """
            RETURN point({longitude:1, latitude:2, crs:'cartesian-3d'}) as result;
            """
        Then an error should be raised

    Scenario: Point creation failure 9:
        When executing query:
            """
            RETURN point({longitude:1, latitude:2, crs:'wgs-84-3d'}) as result;
            """
        Then an error should be raised

    Scenario: Point creation failure 10:
        When executing query:
            """
            RETURN point({longitude:1, latitude:2, height:3, crs:'wgs-84'}) as result;
            """
        Then an error should be raised

    Scenario: Point creation failure 11:
        When executing query:
            """
            RETURN point({longitude:1, latitude:2, height:3, crs:'cartesian'}) as result;
            """
        Then an error should be raised

    Scenario: Point creation failure 12:
        When executing query:
            """
            RETURN point({x:1, y:2, z:3, crs:'cartesian'}) as result;
            """
        Then an error should be raised

    Scenario: Point creation failure 13:
        When executing query:
            """
            RETURN point({x:1, y:2, crs:'cartesian-3d'}) as result;
            """
        Then an error should be raised

    Scenario: Point2d-WGS48 lookup:
        Given an empty graph
        When executing query:
            """
            WITH point({longitude: 1, latitude: 2}) as thing
            RETURN
                thing.x as x_result,
                thing.longitude as longitude_result,
                thing.y as y_result,
                thing.latitude as latitude_result,
                thing.crs as crs_result,
                thing.srid as srid_result;
            """
        Then the result should be:
            | x_result | longitude_result | y_result | latitude_result | crs_result | srid_result |
            | 1.0      | 1.0              | 2.0      | 2.0             | 'wgs-84'   | 4326        |

    Scenario: Point2d-WGS48 lookup z:
        When executing query:
            """
            WITH point({longitude: 1, latitude: 2}) as thing
            RETURN
                thing.z as result;
            """
        Then an error should be raised

    Scenario: Point2d-WGS48 lookup height:
        When executing query:
            """
            WITH point({longitude: 1, latitude: 2}) as thing
            RETURN
                thing.height as result;
            """
        Then an error should be raised

    Scenario: Point3d-WGS48 lookup:
        Given an empty graph
        When executing query:
            """
            WITH point({longitude: 1, latitude: 2, height: 3}) as thing
            RETURN
                thing.x as x_result,
                thing.longitude as longitude_result,
                thing.y as y_result,
                thing.latitude as latitude_result,
                thing.z as z_result,
                thing.height as height_result,
                thing.crs as crs_result,
                thing.srid as srid_result;
            """
        Then the result should be:
            | x_result | longitude_result | y_result | latitude_result | z_result | height_result | crs_result | srid_result |
            | 1.0      | 1.0              | 2.0      | 2.0             | 3.0      | 3.0           | 'wgs-84'   | 4979        |

    Scenario: Point2d-cartesian lookup:
        Given an empty graph
        When executing query:
            """
            WITH point({x: 1, y: 2}) as thing
            RETURN
                thing.x as x_result,
                thing.y as y_result,
                thing.crs as crs_result,
                thing.srid as srid_result;
            """
        Then the result should be:
            | x_result | y_result | crs_result  | srid_result |
            | 1.0      | 2.0      | 'cartesian' | 7203        |

    Scenario: Point2d-cartesian lookup longitude:
        When executing query:
            """
            WITH point({x: 1, y: 2}) as thing RETURN thing.longitude as result;
            """
        Then an error should be raised

    Scenario: Point2d-cartesian lookup latitude:
        When executing query:
            """
            WITH point({x: 1, y: 2}) as thing RETURN thing.latitude as result;
            """
        Then an error should be raised

    Scenario: Point2d-cartesian lookup z:
        When executing query:
            """
            WITH point({x: 1, y: 2}) as thing RETURN thing.z as result;
            """
        Then an error should be raised

    Scenario: Point2d-cartesian lookup height:
        When executing query:
            """
            WITH point({x: 1, y: 2}) as thing RETURN thing.height as result;
            """
        Then an error should be raised

    Scenario: Point3d-cartesian lookup:
        Given an empty graph
        When executing query:
            """
            WITH point({x: 1, y: 2, z:3}) as thing
            RETURN
                thing.x as x_result,
                thing.y as y_result,
                thing.z as z_result,
                thing.crs as crs_result,
                thing.srid as srid_result;
            """
        Then the result should be:
            | x_result | y_result | z_result | crs_result  | srid_result |
            | 1.0      | 2.0      | 3.0      | 'cartesian' | 9157        |

    Scenario: Point3d-cartesian lookup longitude:
        When executing query:
            """
            WITH point({x: 1, y: 2, z:3}) as thing RETURN thing.longitude as result;
            """
        Then an error should be raised

    Scenario: Point3d-cartesian lookup latitude:
        When executing query:
            """
            WITH point({x: 1, y: 2, z:3}) as thing RETURN thing.latitude as result;
            """
        Then an error should be raised

    Scenario: Point3d-cartesian lookup height:
        When executing query:
            """
            WITH point({x: 1, y: 2, z:3}) as thing RETURN thing.height as result;
            """
        Then an error should be raised

    Scenario: Point2d-WGS48 distance:
        When executing query:
            """
            WITH point.distance(
              point({longitude: 12.78, latitude: 56.7}),
              point({longitude: 12.79, latitude: 56.71})
            ) AS dist
            RETURN 1270.8 < dist AND dist < 1271.0 AS result;
            """
        Then the result should be:
            | result |
            | true   |

    Scenario: Point3d-WGS48 distance:
        When executing query:
            """
            WITH point.distance(
              point({longitude: 12.78, latitude: 56.7,   height: 1000}),
              point({longitude: 12.79, latitude: 56.71,  height: 0})
            ) AS dist
            RETURN 1617.1 < dist AND dist < 1617.2 AS result;
            """
        Then the result should be:
            | result |
            | true   |

    Scenario: distance with nulls:
        When executing query:
            """
            RETURN point.distance(null, null) AS result;
            """
        Then the result should be:
            | result |
            | null   |

    Scenario: distance with different crs:
        When executing query:
            """
            RETURN point.distance(
                point({longitude: 12.78, latitude: 56.7,   height: 100}),
                point({longitude: 12.79, latitude: 56.71})
            ) AS result;
            """
        Then the result should be:
            | result |
            | null   |

    Scenario: Point2d-WGS48 withinbbox inside:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 12.5, latitude: 56.5}),
              point({longitude: 12.0, latitude: 56.0}),
              point({longitude: 13.0, latitude: 57.0})
            ) AS result;
            """
        Then the result should be:
            | result |
            | true   |

    Scenario: Point2d-WGS48 withinbbox outside longitude under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 11.0, latitude: 56.5}),
              point({longitude: 12.0, latitude: 56.0}),
              point({longitude: 13.0, latitude: 57.0})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-WGS48 withinbbox outside longitude over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 14.0, latitude: 56.5}),
              point({longitude: 12.0, latitude: 56.0}),
              point({longitude: 13.0, latitude: 57.0})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-WGS48 withinbbox outside latitude under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 12.5, latitude: 55.0}),
              point({longitude: 12.0, latitude: 56.0}),
              point({longitude: 13.0, latitude: 57.0})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-WGS48 withinbbox outside latitude over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 12.5, latitude: 58.0}),
              point({longitude: 12.0, latitude: 56.0}),
              point({longitude: 13.0, latitude: 57.0})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-WGS48 withinbbox wrap around longitude:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  180, latitude: 58.0}),
              point({longitude:  179, latitude: 57.0}),
              point({longitude: -179, latitude: 59.0})
            ) AS result;
            """
        Then the result should be:
            | result |
            | true  |

    Scenario: Point2d-WGS48 withinbbox wrap around longitude negative:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: -180, latitude: 58.0}),
              point({longitude:  179, latitude: 57.0}),
              point({longitude: -179, latitude: 59.0})
            ) AS result;
            """
        Then the result should be:
            | result |
            | true  |

    Scenario: Point2d-WGS48 withinbbox wrap around longitude outside:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  178, latitude: 58.0}),
              point({longitude:  179, latitude: 57.0}),
              point({longitude: -179, latitude: 59.0})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-WGS48 withinbbox wrap around latitude:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  1, latitude: 90}),
              point({longitude:  0, latitude: 89}),
              point({longitude:  2, latitude: -89})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-Cartesian inside:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  1, y: 1}),
              point({x:  0, y: 0}),
              point({x:  2, y: 2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | true  |

    Scenario: Point2d-Cartesian x under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x: -1, y: 1}),
              point({x:  0, y: 0}),
              point({x:  2, y: 2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-Cartesian x over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  3, y: 1}),
              point({x:  0, y: 0}),
              point({x:  2, y: 2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-Cartesian y under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  1, y: -1}),
              point({x:  0, y: 0}),
              point({x:  2, y: 2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point2d-Cartesian y over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  1, y: 3}),
              point({x:  0, y: 0}),
              point({x:  2, y: 2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox inside:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 12.5, latitude: 56.5, height:1}),
              point({longitude: 12.0, latitude: 56.0, height:0}),
              point({longitude: 13.0, latitude: 57.0, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | true   |

    Scenario: Point3d-WGS48 withinbbox outside longitude under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 11.0, latitude: 56.5, height:1}),
              point({longitude: 12.0, latitude: 56.0, height:0}),
              point({longitude: 13.0, latitude: 57.0, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox outside longitude over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 14.0, latitude: 56.5, height:1}),
              point({longitude: 12.0, latitude: 56.0, height:0}),
              point({longitude: 13.0, latitude: 57.0, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox outside latitude under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 12.5, latitude: 55.0, height:1}),
              point({longitude: 12.0, latitude: 56.0, height:0}),
              point({longitude: 13.0, latitude: 57.0, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox outside latitude over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude: 12.5, latitude: 58.0, height:1}),
              point({longitude: 12.0, latitude: 56.0, height:0}),
              point({longitude: 13.0, latitude: 57.0, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox wrap around longitude:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  180, latitude: 58.0, height:1}),
              point({longitude:  179, latitude: 57.0, height:0}),
              point({longitude: -179, latitude: 59.0, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | true  |

    Scenario: Point3d-WGS48 withinbbox wrap around longitude outside:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  178, latitude: 58.0, height:1}),
              point({longitude:  179, latitude: 57.0, height:0}),
              point({longitude: -179, latitude: 59.0, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox wrap around latitude:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  1, latitude: 90, height:1}),
              point({longitude:  0, latitude: 89, height:0}),
              point({longitude:  2, latitude: -89, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox height under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  1, latitude: 1, height:-1}),
              point({longitude:  0, latitude: 0, height:0}),
              point({longitude:  2, latitude: 2, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox height over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  1, latitude: 1, height:3}),
              point({longitude:  0, latitude: 0, height:0}),
              point({longitude:  2, latitude: 2, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-WGS48 withinbbox height boundary:
        When executing query:
            """
            RETURN point.withinbbox(
              point({longitude:  1, latitude: 1, height:2}),
              point({longitude:  0, latitude: 0, height:0}),
              point({longitude:  2, latitude: 2, height:2})
            ) AS result;
            """
        Then the result should be:
            | result |
            | true  |

    Scenario: Point3d-Cartesian withinbbox inside:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  1, y: 1, z:10}),
              point({x:  0, y: 0, z:9}),
              point({x:  2, y: 2, z:11})
            ) AS result;
            """
        Then the result should be:
            | result |
            | true  |

    Scenario: Point3d-Cartesian withinbbox x under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x: -1, y: 1, z:10}),
              point({x:  0, y: 0, z:9}),
              point({x:  2, y: 2, z:11})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-Cartesian withinbbox x over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  3, y: 1, z:10}),
              point({x:  0, y: 0, z:9}),
              point({x:  2, y: 2, z:11})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-Cartesian withinbbox y under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  1, y: -1, z:10}),
              point({x:  0, y: 0,  z:9}),
              point({x:  2, y: 2,  z:11})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-Cartesian withinbbox y over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  1, y: 3, z:10}),
              point({x:  0, y: 0, z:9}),
              point({x:  2, y: 2, z:11})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-Cartesian withinbbox z under:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  1, y: 1,  z:8}),
              point({x:  0, y: 0,  z:9}),
              point({x:  2, y: 2,  z:11})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: Point3d-Cartesian withinbbox z over:
        When executing query:
            """
            RETURN point.withinbbox(
              point({x:  1, y: 1,  z:12}),
              point({x:  0, y: 0,  z:9}),
              point({x:  2, y: 2,  z:11})
            ) AS result;
            """
        Then the result should be:
            | result |
            | false  |

    Scenario: withinbbox with nulls:
        When executing query:
            """
            RETURN point.withinbbox(null, null, null) AS result;
            """
        Then the result should be:
            | result |
            | null   |

    Scenario: withinbbox with different crs:
        When executing query:
            """
            RETURN point.withinbbox(
                point({longitude: 12.78, latitude: 56.7,   height: 100}),
                point({longitude: 12.79, latitude: 56.71}),
                point({longitude: 12.79, latitude: 56.71})
            ) AS result;
            """
        Then the result should be:
            | result |
            | null   |

    Scenario: Create point index:
        Given an empty graph
        And with new point index :L1(prop1)
        And having executed
            """
            CREATE (:L1 {prop1: POINT({x:1, y:1})});
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type | label | property | count |
            | 'point'    | 'L1'  | 'prop1'  | 1     |
        And having executed
            """
            MATCH (n:L1 {prop1: POINT({x:1, y:1})}) DELETE n;
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type | label | property | count |
            | 'point'    | 'L1'  | 'prop1'  | 0     |

    Scenario: Drop point index:
        Given an empty graph
        And having executed
            """
            CREATE POINT INDEX ON :L1(prop1);
            """
        And having executed
            """
            DROP POINT INDEX ON :L1(prop1);
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type             | label | property | count |

    Scenario: Point index with distance cartesian2d:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0}),
                POINT({x: 0, y: 1}),
                POINT({x: 1, y: 0}),
                POINT({x: 1, y: 1}),
                POINT({x: 0.25, y: 0.25}),
                POINT({x: 2, y: 2})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:0,y:0}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) < 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                       |
            | (:L1{prop:POINT({x:0.0, y:0.0, srid: 7203})})                           |
            | (:L1{prop:POINT({x: 0.25, y:0.25, srid: 7203})})                        |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:0,y:0})) < 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                       |
            | (:L1{prop:POINT({x:0.0, y:0.0, srid: 7203})})                           |
            | (:L1{prop:POINT({x: 0.25, y:0.25, srid: 7203})})                        |

        When executing query:
            """
            MATCH (m:L1) WHERE 1 > point.distance(point({x:0,y:0}), m.prop) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                       |
            | (:L1{prop:POINT({x:0.0, y:0.0, srid: 7203})})                           |
            | (:L1{prop:POINT({x: 0.25, y:0.25, srid: 7203})})                        |

        When executing query:
            """
            WITH point({x:0,y:0}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) <= 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                 |
            | (:L1{prop:POINT({x:0.0, y:0.0, srid: 7203})})     |
            | (:L1{prop:POINT({x:0.0, y:1.0, srid:7203})})      |
            | (:L1{prop:POINT({x: 0.25, y:0.25, srid:7203})})   |
            | (:L1{prop:POINT({x: 1.0, y:0.0, srid:7203})})     |

        When executing query:
            """
            MATCH (m:L1) WHERE 1 >= point.distance(m.prop, point({x:0,y:0})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                 |
            | (:L1{prop:POINT({x:0.0, y:0.0, srid: 7203})})     |
            | (:L1{prop:POINT({x:0.0, y:1.0, srid:7203})})      |
            | (:L1{prop:POINT({x: 0.25, y:0.25, srid:7203})})   |
            | (:L1{prop:POINT({x: 1.0, y:0.0, srid:7203})})     |

        When executing query:
            """
            WITH point({x:0,y:0}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) >= 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x: 0.0, y:1.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 1.0, y:0.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 1.0, y:1.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 2.0, y:2.0, srid:7203})})   |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:0,y:0})) >= 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x: 0.0, y:1.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 1.0, y:0.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 1.0, y:1.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 2.0, y:2.0, srid:7203})})   |

        When executing query:
            """
            MATCH (m:L1) WHERE 1 <= point.distance(m.prop, point({x:0,y:0})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x: 0.0, y:1.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 1.0, y:0.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 1.0, y:1.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 2.0, y:2.0, srid:7203})})   |

        When executing query:
            """
            WITH point({x:0,y:0}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) > 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x: 1.0, y:1.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 2.0, y:2.0, srid:7203})})   |

        When executing query:
            """
            MATCH (m:L1) WHERE 1 < point.distance(m.prop, point({x:0,y:0})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x: 1.0, y:1.0, srid:7203})})   |
            | (:L1{prop:POINT({x: 2.0, y:2.0, srid:7203})})   |

    Scenario: Point index with distance cartesian3d:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, z:0}),
                POINT({x: 0, y: 1, z:0}),
                POINT({x: 0, y: 0, z:1}),
                POINT({x: 1, y: 1, z:1}),
                POINT({x: 0.25, y: 0.25, z:0.25}),
                POINT({x: 2, y: 2, z:2})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:0,y:0,z:0}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) < 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                       |
            | (:L1{prop:POINT({x:0.0, y:0.0, z:0.0, srid: 9157})})                    |
            | (:L1{prop:POINT({x: 0.25, y:0.25, z:0.25, srid: 9157})})                |

        When executing query:
            """
            WITH point({x:0,y:0,z:0}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) <= 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                       |
            | (:L1{prop:POINT({x:0.0, y:0.0, z:0.0, srid: 9157})})    |
            | (:L1{prop:POINT({x:0.0, y:0.0, z:1.0, srid:9157})})     |
            | (:L1{prop:POINT({x:0.0, y:1.0, z:0.0, srid:9157})})     |
            | (:L1{prop:POINT({x: 0.25, y:0.25, z:0.25, srid:9157})}) |

        When executing query:
            """
            WITH point({x:0,y:0,z:0}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) >= 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                       |
            | (:L1{prop:POINT({x:0.0, y:0.0, z:1.0, srid:9157})})     |
            | (:L1{prop:POINT({x:0.0, y:1.0, z:0.0, srid:9157})})     |
            | (:L1{prop:POINT({x:1.0, y:1.0, z:1.0, srid: 9157})})    |
            | (:L1{prop:POINT({x:2.0, y:2.0, z:2.0, srid:9157})})     |

        When executing query:
            """
            WITH point({x:0,y:0,z:0}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) > 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                        |
            | (:L1{prop:POINT({x:1.0, y:1.0, z:1.0, srid: 9157})})     |
            | (:L1{prop:POINT({x:2.0, y:2.0, z:2.0, srid:9157})})      |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:0,y:0,z:0})) > 1 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                        |
            | (:L1{prop:POINT({x:1.0, y:1.0, z:1.0, srid: 9157})})     |
            | (:L1{prop:POINT({x:2.0, y:2.0, z:2.0, srid:9157})})      |


    Scenario: Point index scan wgs84 at 0deg boundary:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x:  0, y: 0, crs:"wgs-84"}),
                POINT({x:  1, y: 0, crs:"wgs-84"}),
                POINT({x: -1, y: 0, crs:"wgs-84"}),
                POINT({x: -2, y: 0, crs:"wgs-84"}),
                POINT({x:  2, y: 0, crs:"wgs-84"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:0, y:0, crs:"wgs-84"}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) <= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                          |
            | (:L1{prop:POINT({longitude:-1.0,latitude:0.0,srid:4326})}) |
            | (:L1{prop:POINT({longitude:0.0,latitude:0.0,srid:4326})})  |
            | (:L1{prop:POINT({longitude:1.0,latitude:0.0,srid:4326})})  |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:0, y:0, crs:"wgs-84"})) <= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                          |
            | (:L1{prop:POINT({longitude:-1.0,latitude:0.0,srid:4326})}) |
            | (:L1{prop:POINT({longitude:0.0,latitude:0.0,srid:4326})})  |
            | (:L1{prop:POINT({longitude:1.0,latitude:0.0,srid:4326})})  |

    Scenario: Point index scan wgs84 at 180deg boundary:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x:  180, y: 0, crs:"wgs-84"}),
                POINT({x: -180, y: 0, crs:"wgs-84"}),
                POINT({x: -179, y: 0, crs:"wgs-84"}),
                POINT({x:  179, y: 0, crs:"wgs-84"}),
                POINT({x:  178, y: 0, crs:"wgs-84"}),
                POINT({x: -178, y: 0, crs:"wgs-84"}),
                POINT({x:  180, y: 1, crs:"wgs-84"}),
                POINT({x: -180, y: 1, crs:"wgs-84"}),
                POINT({x: -179, y: 1, crs:"wgs-84"}),
                POINT({x:  179, y: 1, crs:"wgs-84"}),
                POINT({x:  178, y: 1, crs:"wgs-84"}),
                POINT({x: -178, y: 1, crs:"wgs-84"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:180, y:0, crs:"wgs-84"}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) <= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                             |
            | (:L1{prop:POINT({longitude:-180.0,latitude:0.0,srid:4326})})  |
            | (:L1{prop:POINT({longitude:-180.0,latitude:1.0,srid:4326})})  |
            | (:L1{prop:POINT({longitude:-179.0,latitude:0.0,srid:4326})})  |
            | (:L1{prop:POINT({longitude:179.0,latitude:0.0,srid:4326})})   |
            | (:L1{prop:POINT({longitude:180.0,latitude:0.0,srid:4326})})   |
            | (:L1{prop:POINT({longitude:180.0,latitude:1.0,srid:4326})})   |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:180, y:0, crs:"wgs-84"})) <= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                             |
            | (:L1{prop:POINT({longitude:-180.0,latitude:0.0,srid:4326})})  |
            | (:L1{prop:POINT({longitude:-180.0,latitude:1.0,srid:4326})})  |
            | (:L1{prop:POINT({longitude:-179.0,latitude:0.0,srid:4326})})  |
            | (:L1{prop:POINT({longitude:179.0,latitude:0.0,srid:4326})})   |
            | (:L1{prop:POINT({longitude:180.0,latitude:0.0,srid:4326})})   |
            | (:L1{prop:POINT({longitude:180.0,latitude:1.0,srid:4326})})   |


    Scenario: Point index scan wgs84-3d at 0deg boundary:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x:  0, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x:  1, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x: -1, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x:  1, y: 0, z:1000, crs:"wgs-84-3d"}),
                POINT({x: -1, y: 0, z:-1000, crs:"wgs-84-3d"}),
                POINT({x: -2, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x:  2, y: 0, z:0, crs:"wgs-84-3d"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:0, y:0, z:0, crs:"wgs-84-3d"}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) <= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                         |
            | (:L1{prop:POINT({longitude:-1.0, latitude:0.0, height:0.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude: 0.0, latitude:0.0, height:0.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude: 1.0, latitude:0.0, height:0.0, srid:4979})})  |

        When executing query:
            """
            WITH point({x:0, y:0, z:0, crs:"wgs-84-3d"}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) < 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                         |
            | (:L1{prop:POINT({longitude:-1.0, latitude:0.0, height:0.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude: 0.0, latitude:0.0, height:0.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude: 1.0, latitude:0.0, height:0.0, srid:4979})})  |

        When executing query:
            """
            WITH point({x:0, y:0, z:0, crs:"wgs-84-3d"}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) > 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                            |
            | (:L1{prop:POINT({longitude:-2.0,latitude:0.0,height:0.0,srid:4979})})        |
            | (:L1{prop:POINT({longitude:-1.0,latitude:0.0,height:-1000.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude: 1.0, latitude:0.0, height:1000.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude:2.0,latitude:0.0,height:0.0,srid:4979})})         |

        When executing query:
            """
            WITH point({x:0, y:0, z:0, crs:"wgs-84-3d"}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) >= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                            |
            | (:L1{prop:POINT({longitude:-2.0,latitude:0.0,height:0.0,srid:4979})})        |
            | (:L1{prop:POINT({longitude:-1.0,latitude:0.0,height:-1000.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude:0.0,height:1000.0,srid:4979})})     |
            | (:L1{prop:POINT({longitude: 2.0,latitude:0.0,height:0.0,srid:4979})})        |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:0, y:0, z:0, crs:"wgs-84-3d"})) <= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                         |
            | (:L1{prop:POINT({longitude:-1.0, latitude:0.0, height:0.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude: 0.0, latitude:0.0, height:0.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude: 1.0, latitude:0.0, height:0.0, srid:4979})})  |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:0, y:0, z:0, crs:"wgs-84-3d"})) < 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                         |
            | (:L1{prop:POINT({longitude:-1.0, latitude:0.0, height:0.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude: 0.0, latitude:0.0, height:0.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude: 1.0, latitude:0.0, height:0.0, srid:4979})})  |

        When executing query:
            """
            WITH point({x:0, y:0, z:0, crs:"wgs-84-3d"}) AS a MATCH (m:L1) WHERE point.distance(m.prop, point({x:0, y:0, z:0, crs:"wgs-84-3d"})) > 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                            |
            | (:L1{prop:POINT({longitude:-2.0,latitude:0.0,height:0.0,srid:4979})})        |
            | (:L1{prop:POINT({longitude:-1.0,latitude:0.0,height:-1000.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude: 1.0, latitude:0.0, height:1000.0, srid:4979})})  |
            | (:L1{prop:POINT({longitude:2.0,latitude:0.0,height:0.0,srid:4979})})         |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:0, y:0, z:0, crs:"wgs-84-3d"})) >= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                            |
            | (:L1{prop:POINT({longitude:-2.0,latitude:0.0,height:0.0,srid:4979})})        |
            | (:L1{prop:POINT({longitude:-1.0,latitude:0.0,height:-1000.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude:0.0,height:1000.0,srid:4979})})     |
            | (:L1{prop:POINT({longitude: 2.0,latitude:0.0,height:0.0,srid:4979})})        |

    Scenario: Point index scan wgs84-3d at 180deg boundary:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x:  180, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x: -180, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x: -179, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x:  179, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x:  178, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x: -178, y: 0, z:0, crs:"wgs-84-3d"}),
                POINT({x:  180, y: 1, z:0, crs:"wgs-84-3d"}),
                POINT({x: -180, y: 1, z:0, crs:"wgs-84-3d"}),
                POINT({x: -179, y: 1, z:0, crs:"wgs-84-3d"}),
                POINT({x:  179, y: 1, z:0, crs:"wgs-84-3d"}),
                POINT({x:  178, y: 1, z:0, crs:"wgs-84-3d"}),
                POINT({x: -178, y: 1, z:0, crs:"wgs-84-3d"}),
                POINT({x: -179, y: 0, z:-100, crs:"wgs-84-3d"}),
                POINT({x: -179, y: 0, z:100, crs:"wgs-84-3d"}),
                POINT({x: 179, y: 0, z:-100, crs:"wgs-84-3d"}),
                POINT({x: 179, y: 0, z:100, crs:"wgs-84-3d"}),
                POINT({x: 179, y: 0, z:1000, crs:"wgs-84-3d"}),
                POINT({x: 179, y: 0, z:-1000, crs:"wgs-84-3d"}),
                POINT({x: -179, y: 0, z:1000, crs:"wgs-84-3d"}),
                POINT({x: -179, y: 0, z:-1000, crs:"wgs-84-3d"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:180, y:0, z:0, crs:"wgs-84-3d"}) AS a MATCH (m:L1) WHERE point.distance(m.prop, a) <= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                          |
            | (:L1{prop:POINT({longitude:-180.0,latitude:0.0,height:0.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude:-180.0,latitude:1.0,height:0.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude:0.0,height:-100.0,srid:4979})}) |
            | (:L1{prop:POINT({longitude:-179.0,latitude:0.0,height:0.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude:0.0,height:100.0,srid:4979})})  |
            | (:L1{prop:POINT({longitude:179.0,latitude:0.0,height:-100.0,srid:4979})})  |
            | (:L1{prop:POINT({longitude:179.0,latitude:0.0,height:0.0,srid:4979})})     |
            | (:L1{prop:POINT({longitude:179.0,latitude:0.0,height:100.0,srid:4979})})   |
            | (:L1{prop:POINT({longitude:180.0,latitude:0.0,height:0.0,srid:4979})})     |
            | (:L1{prop:POINT({longitude:180.0,latitude:1.0,height:0.0,srid:4979})})     |

        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(m.prop, point({x:180, y:0, z:0, crs:"wgs-84-3d"})) <= 111320 RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                          |
            | (:L1{prop:POINT({longitude:-180.0,latitude:0.0,height:0.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude:-180.0,latitude:1.0,height:0.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude:0.0,height:-100.0,srid:4979})}) |
            | (:L1{prop:POINT({longitude:-179.0,latitude:0.0,height:0.0,srid:4979})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude:0.0,height:100.0,srid:4979})})  |
            | (:L1{prop:POINT({longitude:179.0,latitude:0.0,height:-100.0,srid:4979})})  |
            | (:L1{prop:POINT({longitude:179.0,latitude:0.0,height:0.0,srid:4979})})     |
            | (:L1{prop:POINT({longitude:179.0,latitude:0.0,height:100.0,srid:4979})})   |
            | (:L1{prop:POINT({longitude:180.0,latitude:0.0,height:0.0,srid:4979})})     |
            | (:L1{prop:POINT({longitude:180.0,latitude:1.0,height:0.0,srid:4979})})     |

    Scenario: Point index scan wgs84-2d withinbbox:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, crs:"wgs-84"}),
                POINT({x: 0, y: 1, crs:"wgs-84"}),
                POINT({x: 1, y: 0, crs:"wgs-84"}),
                POINT({x: 1, y: 1, crs:"wgs-84"}),
                POINT({x:-1, y:-1, crs:"wgs-84"}),
                POINT({x: 2, y: 2, crs:"wgs-84"}),
                POINT({x:-2, y:-2, crs:"wgs-84"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, crs:"wgs-84"}) AS lb, point({x:1, y:1, crs:"wgs-84"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                              |
            | (:L1{prop:POINT({longitude:-1.0,latitude:-1.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 0.0,latitude: 0.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 0.0,latitude: 1.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude: 0.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude: 1.0,srid:4326})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:-1, y:-1, crs:"wgs-84"}), point({x:1, y:1, crs:"wgs-84"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                              |
            | (:L1{prop:POINT({longitude:-1.0,latitude:-1.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 0.0,latitude: 0.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 0.0,latitude: 1.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude: 0.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude: 1.0,srid:4326})})    |

    Scenario: Point index scan wgs84-3d withinbbox:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, z: 0, crs:"wgs-84-3d"}),
                POINT({x: 0, y: 1, z: 2, crs:"wgs-84-3d"}),
                POINT({x: 1, y: 0, z: 0, crs:"wgs-84-3d"}),
                POINT({x: 1, y: 1, z: 1, crs:"wgs-84-3d"}),
                POINT({x:-1, y:-1, z:-1, crs:"wgs-84-3d"}),
                POINT({x: 2, y: 2, z: 0, crs:"wgs-84-3d"}),
                POINT({x:-2, y:-2, z: 0, crs:"wgs-84-3d"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, z:-1, crs:"wgs-84-3d"}) AS lb, point({x:1, y:1, z:1, crs:"wgs-84-3d"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                           |
            | (:L1{prop:POINT({longitude:-1.0,latitude:-1.0, height:-1.0, srid:4979})})   |
            | (:L1{prop:POINT({longitude: 0.0,latitude: 0.0, height:0.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude: 0.0, height:0.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude: 1.0, height: 1.0, srid:4979})})   |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:-1, y:-1, z:-1, crs:"wgs-84-3d"}), point({x:1, y:1, z:1, crs:"wgs-84-3d"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                                           |
            | (:L1{prop:POINT({longitude:-1.0,latitude:-1.0, height:-1.0, srid:4979})})   |
            | (:L1{prop:POINT({longitude: 0.0,latitude: 0.0, height:0.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude: 0.0, height:0.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude: 1.0,latitude: 1.0, height: 1.0, srid:4979})})   |

    Scenario: Point index scan cartesian-2d withinbbox:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, crs:"cartesian"}),
                POINT({x: 0, y: 1, crs:"cartesian"}),
                POINT({x: 1, y: 0, crs:"cartesian"}),
                POINT({x: 1, y: 1, crs:"cartesian"}),
                POINT({x:-1, y:-1, crs:"cartesian"}),
                POINT({x: 2, y: 2, crs:"cartesian"}),
                POINT({x:-2, y:-2, crs:"cartesian"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, crs:"cartesian"}) AS lb, point({x:1, y:1, crs:"cartesian"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-1.0,y:-1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0,srid:7203})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:-1, y:-1, crs:"cartesian"}), point({x:1, y:1, crs:"cartesian"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-1.0,y:-1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0,srid:7203})})    |

    Scenario: Point index scan cartesian-3d withinbbox:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, z: 0, crs:"cartesian-3d"}),
                POINT({x: 0, y: 1, z: 2, crs:"cartesian-3d"}),
                POINT({x: 1, y: 0, z: 0, crs:"cartesian-3d"}),
                POINT({x: 1, y: 1, z: 1, crs:"cartesian-3d"}),
                POINT({x:-1, y:-1, z:-1, crs:"cartesian-3d"}),
                POINT({x: 2, y: 2, z: 0, crs:"cartesian-3d"}),
                POINT({x:-2, y:-2, z: 0, crs:"cartesian-3d"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, z:-1, crs:"cartesian-3d"}) AS lb, point({x:1, y:1, z:1, crs:"cartesian-3d"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                        |
            | (:L1{prop:POINT({x:-1.0,y:-1.0, z:-1.0, srid:9157})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0, z: 0.0, srid:9157})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0, z: 0.0, srid:9157})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0, z: 1.0, srid:9157})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:-1, y:-1, z:-1, crs:"cartesian-3d"}), point({x:1, y:1, z:1, crs:"cartesian-3d"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC, m.prop.z ASC;
            """
        Then the result should be:
            | m                                                        |
            | (:L1{prop:POINT({x:-1.0,y:-1.0, z:-1.0, srid:9157})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0, z: 0.0, srid:9157})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0, z: 0.0, srid:9157})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0, z: 1.0, srid:9157})})    |

    Scenario: Point index scan withinbbox implied true:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, crs:"cartesian"}),
                POINT({x: 0, y: 1, crs:"cartesian"}),
                POINT({x: 1, y: 0, crs:"cartesian"}),
                POINT({x: 1, y: 1, crs:"cartesian"}),
                POINT({x:-1, y:-1, crs:"cartesian"}),
                POINT({x: 2, y: 2, crs:"cartesian"}),
                POINT({x:-2, y:-2, crs:"cartesian"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, crs:"cartesian"}) AS lb, point({x:1, y:1, crs:"cartesian"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-1.0,y:-1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0,srid:7203})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:-1, y:-1, crs:"cartesian"}), point({x:1, y:1, crs:"cartesian"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-1.0,y:-1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0,srid:7203})})    |

    Scenario: Point index scan withinbbox implied false:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, crs:"cartesian"}),
                POINT({x: 0, y: 1, crs:"cartesian"}),
                POINT({x: 1, y: 0, crs:"cartesian"}),
                POINT({x: 1, y: 1, crs:"cartesian"}),
                POINT({x:-1, y:-1, crs:"cartesian"}),
                POINT({x: 2, y: 2, crs:"cartesian"}),
                POINT({x:-2, y:-2, crs:"cartesian"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, crs:"cartesian"}) AS lb, point({x:1, y:1, crs:"cartesian"}) AS up MATCH (m:L1) WHERE NOT point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-2.0,y:-2.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 2.0,y: 2.0,srid:7203})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE NOT point.withinbbox(m.prop, point({x:-1, y:-1, crs:"cartesian"}), point({x:1, y:1, crs:"cartesian"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-2.0,y:-2.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 2.0,y: 2.0,srid:7203})})    |

    Scenario: Point index scan withinbbox equals true:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, crs:"cartesian"}),
                POINT({x: 0, y: 1, crs:"cartesian"}),
                POINT({x: 1, y: 0, crs:"cartesian"}),
                POINT({x: 1, y: 1, crs:"cartesian"}),
                POINT({x:-1, y:-1, crs:"cartesian"}),
                POINT({x: 2, y: 2, crs:"cartesian"}),
                POINT({x:-2, y:-2, crs:"cartesian"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, crs:"cartesian"}) AS lb, point({x:1, y:1, crs:"cartesian"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) = true RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-1.0,y:-1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0,srid:7203})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:-1, y:-1, crs:"cartesian"}), point({x:1, y:1, crs:"cartesian"})) = true RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-1.0,y:-1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0,srid:7203})})    |

    Scenario: Point index scan withinbbox equals false:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, crs:"cartesian"}),
                POINT({x: 0, y: 1, crs:"cartesian"}),
                POINT({x: 1, y: 0, crs:"cartesian"}),
                POINT({x: 1, y: 1, crs:"cartesian"}),
                POINT({x:-1, y:-1, crs:"cartesian"}),
                POINT({x: 2, y: 2, crs:"cartesian"}),
                POINT({x:-2, y:-2, crs:"cartesian"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, crs:"cartesian"}) AS lb, point({x:1, y:1, crs:"cartesian"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) = false RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-2.0,y:-2.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 2.0,y: 2.0,srid:7203})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE false = point.withinbbox(m.prop, point({x:-1, y:-1, crs:"cartesian"}), point({x:1, y:1, crs:"cartesian"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-2.0,y:-2.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 2.0,y: 2.0,srid:7203})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:-1, y:-1, crs:"cartesian"}), point({x:1, y:1, crs:"cartesian"})) = false RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-2.0,y:-2.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 2.0,y: 2.0,srid:7203})})    |

    Scenario: Point index scan wgs84-2d withinbbox dateline:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 179, y: 0, crs:"wgs-84"}),
                POINT({x: 180, y: 1, crs:"wgs-84"}),
                POINT({x:-179, y: 0, crs:"wgs-84"}),
                POINT({x:-180, y: 1, crs:"wgs-84"}),
                POINT({x:-179, y:-1, crs:"wgs-84"}),
                POINT({x:-179, y: 2, crs:"wgs-84"}),
                POINT({x:-178, y: 0, crs:"wgs-84"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:179, y:-1, crs:"wgs-84"}) AS lb, point({x:-179, y:1, crs:"wgs-84"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                |
            | (:L1{prop:POINT({longitude:-180.0,latitude: 1.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude:-1.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude: 0.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 179.0,latitude: 0.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 180.0,latitude: 1.0,srid:4326})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:179, y:-1, crs:"wgs-84"}), point({x:-179, y:1, crs:"wgs-84"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                |
            | (:L1{prop:POINT({longitude:-180.0,latitude: 1.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude:-1.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude: 0.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 179.0,latitude: 0.0,srid:4326})})    |
            | (:L1{prop:POINT({longitude: 180.0,latitude: 1.0,srid:4326})})    |

    Scenario: Point index scan wgs84-3d withinbbox dateline:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 179, y: 0, z: 0, crs:"wgs-84-3d"}),
                POINT({x: 180, y: 1, z: 1, crs:"wgs-84-3d"}),
                POINT({x:-179, y: 0, z:-2, crs:"wgs-84-3d"}),
                POINT({x:-180, y: 1, z: 1, crs:"wgs-84-3d"}),
                POINT({x:-179, y:-1, z: 0, crs:"wgs-84-3d"}),
                POINT({x:-179, y: 2, z: 1, crs:"wgs-84-3d"}),
                POINT({x:-178, y: 0, z: 0, crs:"wgs-84-3d"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:179, y:-1, z:-1, crs:"wgs-84-3d"}) AS lb, point({x:-179, y:1, z:1, crs:"wgs-84-3d"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                             |
            | (:L1{prop:POINT({longitude:-180.0,latitude: 1.0, height:1.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude:-1.0, height:0.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude: 179.0,latitude: 0.0, height:0.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude: 180.0,latitude: 1.0, height:1.0, srid:4979})})    |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:179, y:-1, z:-1, crs:"wgs-84-3d"}), point({x:-179, y:1, z:1, crs:"wgs-84-3d"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                             |
            | (:L1{prop:POINT({longitude:-180.0,latitude: 1.0, height:1.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude:-179.0,latitude:-1.0, height:0.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude: 179.0,latitude: 0.0, height:0.0, srid:4979})})    |
            | (:L1{prop:POINT({longitude: 180.0,latitude: 1.0, height:1.0, srid:4979})})    |

    Scenario: Point index scan cartesian-2d withinbbox wrap fails:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, crs:"cartesian"}),
                POINT({x: 1, y: 1, crs:"cartesian"}),
                POINT({x: 2, y: 2, crs:"cartesian"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:1, y:1, crs:"cartesian"}) AS lb, point({x:-1, y:-1, crs:"cartesian"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:1, y:1, crs:"cartesian"}), point({x:-1, y:-1, crs:"cartesian"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                |

    Scenario: Point index scan cartesian-3d withinbbox wrap fails:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, z:0, crs:"cartesian-3d"}),
                POINT({x: 1, y: 1, z:1, crs:"cartesian-3d"}),
                POINT({x: 2, y: 2, z:2, crs:"cartesian-3d"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:1, y:1, z:1, crs:"cartesian-3d"}) AS lb, point({x:-1, y:-1, z:-1, crs:"cartesian-3d"}) AS up MATCH (m:L1) WHERE point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                |

        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(m.prop, point({x:1, y:1, z:1, crs:"cartesian-3d"}), point({x:-1, y:-1, z:-1, crs:"cartesian-3d"})) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                                                |

    Scenario: Point index scan withinbbox commutation:
        Given an empty graph
        And with new point index :L1(prop)
        And having executed
            """
            UNWIND [
                POINT({x: 0, y: 0, crs:"cartesian"}),
                POINT({x: 0, y: 1, crs:"cartesian"}),
                POINT({x: 1, y: 0, crs:"cartesian"}),
                POINT({x: 1, y: 1, crs:"cartesian"}),
                POINT({x:-1, y:-1, crs:"cartesian"}),
                POINT({x: 2, y: 2, crs:"cartesian"}),
                POINT({x:-2, y:-2, crs:"cartesian"})
            ] AS point
            CREATE (:L1 {prop: point});
            """
        When executing query:
            """
            WITH point({x:-1, y:-1, crs:"cartesian"}) AS lb, point({x:1, y:1, crs:"cartesian"}) AS up MATCH (m:L1) WHERE true = point.withinbbox(m.prop, lb, up) RETURN m ORDER BY m.prop.x ASC, m.prop.y ASC;
            """
        Then the result should be:
            | m                                               |
            | (:L1{prop:POINT({x:-1.0,y:-1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 0.0,y: 1.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 0.0,srid:7203})})    |
            | (:L1{prop:POINT({x: 1.0,y: 1.0,srid:7203})})    |

    Scenario: Point index with distance wrong arguments:
        When executing query:
            """
            MATCH (m:L1) WHERE point.distance(point({x:0,y:0}, m.prop)) < 1 RETURN m;
            """
        Then an error should be raised

    Scenario: Point index with withinbbox wrong arguments:
        When executing query:
            """
            MATCH (m:L1) WHERE point.withinbbox(point({x:0,y:0}, m.prop), point({x:0, y:0})) RETURN m;
            """
        Then an error should be raised
