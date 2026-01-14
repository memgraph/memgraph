setup: |-
    CALL link_prediction.set_model_parameters({split_ratio: 0.8, hidden_features_size: [3, 2], target_relation: "CITES", add_reverse_edges: False}) YIELD *;
queries:
    - |-
        CREATE (v1:PAPER {id: 10, features: [1, 2, 3]});
        CREATE (v2:PAPER {id: 11, features: [1.54, 0.3, 1.78]});
        MATCH (v1:PAPER {id: 10}), (v2:PAPER {id: 11}) CREATE (v1)-[e:CITES {}]->(v2);
cleanup: |-
    CALL link_prediction.reset_parameters() YIELD *;
