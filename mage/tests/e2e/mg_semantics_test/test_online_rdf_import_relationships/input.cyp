setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CALL mg_semantics.rdf_import_inline('<http://example.org/Alice> <http://schema.org/knows> <http://example.org/Bob> .', 'N-Triples');
