setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CALL mg_semantics.mapping_add('http://schema.org/name', 'personName');
    - |-
        CALL mg_semantics.mapping_drop('personName');
