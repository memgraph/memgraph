CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__ {__mg_id__: 0, `id`: "0", `value_str`: "hello", `value_char`: "world", `value_int`: 1, `value_long`: 2, `value_byte`: 3, `value_short`: 4, `value_double`: 3.141, `value_float`: 2.718, `value_integer`: 5, `value_boolean`: true, `value_bool`: false});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;
