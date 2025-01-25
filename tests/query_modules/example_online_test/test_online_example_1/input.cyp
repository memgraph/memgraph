setup: |-
    CREATE (a:Device {id: 1, status: 'active'});
queries:
    - |-
        MATCH (n:Device {id: 1}) SET n.status = 'inactive';
    - |-
        CREATE (a:Device {id: 2, status: 'active'});

cleanup: |-
    MATCH (n) DETACH DELETE n;
