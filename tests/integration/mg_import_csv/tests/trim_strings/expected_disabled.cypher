CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 0, `content`: "yes", `browser`: "Chrome", `country`: "  Croatia ", `id`: "0"});
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 1, `content`: "thanks", `browser`: "Chrome", `country`: "United Kingdom", `id`: "1"});
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 2, `content`: "LOL", `browser`: "  ", `country`: "Germany", `id`: "2"});
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 3, `content`: " I see     ", `browser`: "Firefox", `country`: "France", `id`: "3"});
CREATE (:__mg_vertex__:`Message`:`Comment` {__mg_id__: 4, `content`: " fine", `browser`: "Internet Explorer ", `country`: "Italy", `id`: "4"});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;