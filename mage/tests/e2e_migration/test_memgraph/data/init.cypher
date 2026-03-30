MATCH (n:DummyNode) DETACH DELETE n;
CREATE (n:DummyNode {integer_col: 2147483647, float_col: 3.14159, boolean_col: true, string_col: 'Hello Memgraph', list_col: [1, 2, 3, 4, 5], date_col: date('2023-12-25'), localtime_col: localTime('10:30:45'), localdatetime_col: localDateTime('2023-12-25T10:30:45'), duration_col: duration('P1DT2H3M4S')});
