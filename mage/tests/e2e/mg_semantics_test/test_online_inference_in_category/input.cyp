setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CREATE (cat:Resource {uri: 'http://example.org/Science', name: 'Science'});
        CREATE (item:Resource {uri: 'http://example.org/Physics', name: 'Physics'});
        MATCH (item {uri: 'http://example.org/Physics'}), (cat {uri: 'http://example.org/Science'}) CREATE (item)-[:IN_CATEGORY]->(cat);
