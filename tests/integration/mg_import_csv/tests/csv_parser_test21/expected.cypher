CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__ {__mg_id__: 0, `test`: "asd", `value`: "\"worldÖ\"Ö\"\"Ö f\"Ögh", `id`: "0"});
CREATE (:__mg_vertex__ {__mg_id__: 1, `test`: "string", `id`: "1"});
CREATE (:__mg_vertex__ {__mg_id__: 2, `test`: "wil\"l tÖhis work?\"", `value`: "hello", `id`: "2"});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;