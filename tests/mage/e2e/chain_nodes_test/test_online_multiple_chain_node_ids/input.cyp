queries:
    - |-
        CREATE (:A {id:1}), (:B {id:2}), (:C {id:3}), (:D {id:4});
        MATCH (n) WITH collect(n) as nodes CALL graph_util.chain_nodes(nodes, "CONNECTED_TO") YIELD connections RETURN connections;
