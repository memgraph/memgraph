CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__ {__mg_id__: 0, `test`: "asd", `id`: "0", `value`: "\"worldÖ\"Ö\"\"Ö f\"Ögh"});
CREATE (:__mg_vertex__ {__mg_id__: 1, `test`: "string", `id`: "1"});
CREATE (:__mg_vertex__ {__mg_id__: 2, `test`: "wil\"l tÖhis work?\"", `id`: "2", `value`: "hello"});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;
