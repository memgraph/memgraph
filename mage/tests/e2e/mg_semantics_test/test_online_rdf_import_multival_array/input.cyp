setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init({handleMultival: "ARRAY", handleVocabUris: "IGNORE"});
    - |-
        CALL mg_semantics.rdf_import_inline('<http://example.org/Item> <http://schema.org/name> "Name1" .\n<http://example.org/Item> <http://schema.org/name> "Name2" .', 'N-Triples');
