CREATE CONSTRAINT ON (u:label) ASSERT EXISTS (u.prop);
CREATE CONSTRAINT ON (u:label2) ASSERT EXISTS (u.prop1);
CREATE CONSTRAINT ON (u:labellabel) ASSERT EXISTS (u.prop);
CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__:label {__mg_id__: 0, prop: 1});
CREATE (:__mg_vertex__:label {__mg_id__: 1, prop: false});
CREATE (:__mg_vertex__:label2 {__mg_id__: 2, prop1: 1});
CREATE (:__mg_vertex__:label2 {__mg_id__: 3, prop1: 2});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;
