setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CALL mg_semantics.rdf_stream_inline('<http://example.org/A> <http://schema.org/name> "Alice" .', 'N-Triples') YIELD subject RETURN subject;
