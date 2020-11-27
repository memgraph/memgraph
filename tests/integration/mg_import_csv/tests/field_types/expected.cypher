CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__ {__mg_id__: 0, `value_bool`: false, `value_boolean`: true, `value_integer`: 5, `value_float`: 2.718, `value_double`: 3.141, `value_short`: 4, `value_byte`: 3, `value_long`: 2, `value_int`: 1, `value_char`: "world", `value_str`: "hello", `id`: "0"});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;