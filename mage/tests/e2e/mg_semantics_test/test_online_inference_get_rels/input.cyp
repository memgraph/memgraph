setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CREATE (a:Resource {uri: 'http://example.org/A', name: 'A'});
        CREATE (b:Resource {uri: 'http://example.org/B', name: 'B'});
        MATCH (a {uri: 'http://example.org/A'}), (b {uri: 'http://example.org/B'}) CREATE (a)-[:KNOWS]->(b);
