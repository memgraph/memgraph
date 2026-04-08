setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CREATE (a:Resource:Dog {uri: 'http://example.org/Buddy', name: 'Buddy'});
        CREATE (b:Resource:Cat {uri: 'http://example.org/Whiskers', name: 'Whiskers'});
