// --storage-items-per-batch is set to 5
CREATE INDEX ON :__mg_vertex__(__mg_id__);
CREATE (:__mg_vertex__ {__mg_id__: 0});
CREATE (:__mg_vertex__:`label` {__mg_id__: 1});
CREATE (:__mg_vertex__:`label` {__mg_id__: 2, `prop`: false});
CREATE (:__mg_vertex__:`label` {__mg_id__: 3, `prop`: true});
CREATE (:__mg_vertex__:`label2` {__mg_id__: 4, `prop`: 1});
CREATE (:__mg_vertex__:`label` {__mg_id__: 5, `prop2`: 3.141});
CREATE (:__mg_vertex__:`label6` {__mg_id__: 6, `prop3`: true, `prop2`: -314000000});
CREATE (:__mg_vertex__:`label3`:`label1`:`label2` {__mg_id__: 7});
CREATE (:__mg_vertex__:`label` {__mg_id__: 8, `prop3`: "str", `prop2`: 2, `prop`: 1});
CREATE (:__mg_vertex__:`label2`:`label1` {__mg_id__: 9, `prop`: {`prop_nes`: "kaj je"}});
CREATE (:__mg_vertex__:`label` {__mg_id__: 10, `prop_array`: [1, false, Null, "str", {`prop2`: 2}]});
CREATE (:__mg_vertex__:`label3`:`label` {__mg_id__: 11, `prop`: {`prop`: [1, false], `prop2`: {}, `prop3`: "test2", `prop4`: "test"}});
CREATE (:__mg_vertex__ {__mg_id__: 12, `prop`: " \n\"\'\t\\%"});
DROP INDEX ON :__mg_vertex__(__mg_id__);
MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;
