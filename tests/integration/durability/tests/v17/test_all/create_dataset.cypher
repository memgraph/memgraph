// --storage-items-per-batch is set to 10
CREATE INDEX ON :`label2`(`prop2`);
CREATE INDEX ON :`label2`(`prop`);
CREATE INDEX ON :`label`;
CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__:`label2` {__mg_id__: 0, `prop2`: ["kaj", 2, Null, {`prop4`: -1.341}], `ext`: 2, `prop`: "joj"});
CREATE (:__mg_vertex__:`label2`:`label` {__mg_id__: 1, `ext`: 2, `prop`: "joj"});
CREATE (:__mg_vertex__:`label2` {__mg_id__: 2, `prop2`: 2, `prop`: 1});
CREATE (:__mg_vertex__:`label2` {__mg_id__: 3, `prop2`: 2, `prop`: 2});
MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 0 CREATE (u)-[:`link` {`ext`: [false, {`k`: "l"}], `prop`: -1}]->(v);
MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 1 CREATE (u)-[:`link` {`ext`: [false, {`k`: "l"}], `prop`: -1}]->(v);
MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 2 CREATE (u)-[:`link` {`ext`: [false, {`k`: "l"}], `prop`: -1}]->(v);
MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 3 CREATE (u)-[:`link` {`ext`: [false, {`k`: "l"}], `prop`: -1}]->(v);
CREATE CONSTRAINT ON (u:`label`) ASSERT EXISTS (u.`ext`);
CREATE CONSTRAINT ON (u:`label2`) ASSERT u.`prop2`, u.`prop` IS UNIQUE;
ANALYZE GRAPH;
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;
