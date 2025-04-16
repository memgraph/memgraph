CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__ {__mg_id__: 0, `id`: "0", `value`: "\"world,\",\"\", f\",gh", `test`: "asd"});
CREATE (:__mg_vertex__ {__mg_id__: 1, `id`: "1", `test`: "string"});
CREATE (:__mg_vertex__ {__mg_id__: 2, `id`: "2", `value`: "hello", `test`: "wil\"l t,his work?\""});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;
