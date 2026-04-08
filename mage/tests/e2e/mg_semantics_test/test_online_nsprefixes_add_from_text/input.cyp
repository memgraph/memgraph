setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CALL mg_semantics.nsprefixes_remove_all();
    - |-
        CALL mg_semantics.nsprefixes_add_from_text('@prefix foaf: <http://xmlns.com/foaf/0.1/> . @prefix dc: <http://purl.org/dc/elements/1.1/> .');
