CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 0, `id`: "0", `country`: "  Croatia ", `browser`: "Chrome", `content`: "yes"});
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 1, `id`: "1", `country`: "United Kingdom", `browser`: "Chrome", `content`: "thanks"});
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 2, `id`: "2", `country`: "Germany", `browser`: "  ", `content`: "LOL"});
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 3, `id`: "3", `country`: "France", `browser`: "Firefox", `content`: " I see     "});
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 4, `id`: "4", `country`: "Italy", `browser`: "Internet Explorer ", `content`: " fine"});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;