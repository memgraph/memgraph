queries:
    - |-
        CREATE (a:Node {id: 0})-[:R {w: 1}]->(b:Node {id: 1}) CREATE (:Node {id: 2})
    - |-
        MATCH (n) RETURN n;
