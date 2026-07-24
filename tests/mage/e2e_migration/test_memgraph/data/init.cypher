MATCH (n:DummyNode) DETACH DELETE n;
CREATE (n:DummyNode {integer_col: 2147483647, float_col: 3.14159, boolean_col: true, string_col: 'Hello Memgraph', list_col: [1, 2, 3, 4, 5], map_col: {key1: 'value1', key2: 42}, null_col: null, date_col: date('2023-12-25'), localtime_col: localTime('10:30:45'), localdatetime_col: localDateTime('2023-12-25T10:30:45'), duration_col: duration('P1DT2H3M4S'), zoneddatetime_col: datetime('2023-12-25T10:30:45+01:00'), point_2d_col: point({x: 1.0, y: 2.0}), point_3d_col: point({x: 1.0, y: 2.0, z: 3.0})});
CREATE ENUM Status VALUES { Good, Okay, Bad };
CREATE (e:DummyNodeEnum {enum_col: Status::Good});
CREATE (a:DummyNode2)-[:DUMMY_REL]->(b:DummyNode2);
