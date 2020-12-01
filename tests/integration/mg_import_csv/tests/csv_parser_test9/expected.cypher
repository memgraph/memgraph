CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__ {__mg_id__: 0, `id`: "0"});
CREATE (:__mg_vertex__ {__mg_id__: 1, `id`: "1"});
CREATE (:__mg_vertex__ {__mg_id__: 2, `value`: "hello", `id`: "2"});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;