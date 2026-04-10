setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init({handleVocabUris: "IGNORE"});
    - |-
        CALL mg_semantics.rdf_import_inline('<http://example.org/Alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .\n<http://example.org/Alice> <http://schema.org/name> "Alice" .', 'N-Triples');
