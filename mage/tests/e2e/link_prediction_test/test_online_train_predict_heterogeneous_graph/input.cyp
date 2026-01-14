setup: |-
    CALL link_prediction.set_model_parameters({split_ratio: 1.0, hidden_features_size: [3, 2], target_relation: "SUBSCRIBES_TO", add_reverse_edges: False}) YIELD *;

queries:
    - |-
        CREATE (v1:Customer {id: 10, features: [1, 2, 3]});
        CREATE (v2:Customer {id: 11, features: [1.54, 0.3, 1.78]});
        CREATE (v3:Service {id: 12, features: [0.5, 1, 4.5]});
        MATCH (v1:Customer {id: 10}), (v2:Customer {id: 11}) CREATE (v1)-[e:CONNECTS_TO {}]->(v2);
        MATCH (v2:Customer {id: 11}), (v3:Service {id: 12}) CREATE (v2)-[e:SUBSCRIBES_TO {}]->(v3);
        MATCH (v1:Customer {id: 10}), (v3:Service {id: 12}) CREATE (v1)-[e:USED_BY {}]->(v3);
    - |-
        RETURN 1;
cleanup: |-
    CALL link_prediction.reset_parameters() YIELD *;
