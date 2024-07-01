// --storage-items-per-batch is set to 10
CREATE INDEX ON :`label2`(`prop2`);
CREATE INDEX ON :`label2`(`prop`);
CREATE INDEX ON :`label`;
CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE EDGE INDEX ON :`edge_type`;
CREATE EDGE INDEX ON :`edge_type_tmp`;
CREATE EDGE INDEX ON :`edge_type`(`prop`);
CREATE EDGE INDEX ON :`edge_type_tmp`(`prop`);
CREATE (:`edge_index_from`), (:`edge_index_to`);
MATCH (n:`edge_index_from`), (m:`edge_index_to`) CREATE (n)-[r:`edge_type`]->(m);
MATCH (n:`edge_index_from`), (m:`edge_index_to`) CREATE (n)-[r:`edge_type` {`prop`: 1}]->(m);
CREATE (:__mg_vertex__:`label2` {__mg_id__: 0, `prop2`: ["kaj", 2, Null, {`prop4`: -1.341}], `ext`: 2, `prop`: "joj"});
CREATE (:__mg_vertex__:`label2`:`label` {__mg_id__: 1, `ext`: 2, `prop`: "joj"});
CREATE (:__mg_vertex__:`label2` {__mg_id__: 2, `prop2`: 2, `prop`: 1});
CREATE (:__mg_vertex__:`label2` {__mg_id__: 3, `prop2`: 2, `prop`: 2});
MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 0 CREATE (u)-[:`link` {`ext`: [false, {`k`: "l"}], `prop`: -1}]->(v);
MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 1 CREATE (u)-[:`link` {`ext`: [false, {`k`: "l"}], `prop`: -1}]->(v);
MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 2 CREATE (u)-[:`link` {`ext`: [false, {`k`: "l"}], `prop`: -1}]->(v);
MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 3 CREATE (u)-[:`link` {`ext`: [false, {`k`: "l"}], `prop`: -1}]->(v);
MATCH ()-[r:`edge_type`]->() RETURN r;
MATCH ()-[r:`edge_type` {`prop`: 1}]->() RETURN r;
CREATE CONSTRAINT ON (u:`label`) ASSERT EXISTS (u.`ext`);
CREATE CONSTRAINT ON (u:`label2`) ASSERT u.`prop2`, u.`prop` IS UNIQUE;
ANALYZE GRAPH;
DROP INDEX ON :__mg_vertex__(__mg_id__);
DROP EDGE INDEX ON :`edge_type_tmp`;
DROP EDGE INDEX ON :`edge_type_tmp`(`prop`);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;