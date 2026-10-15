setup: |-
    CALL link_prediction.set_model_parameters({hidden_features_size: [3, 2], split_ratio: 1.0, target_relation: "CITES", add_reverse_edges: False}) YIELD *
queries:
    - |-
        CREATE (v1:PAPER {id: 10, features: [1, 2, 3]});
        CREATE (v2:PAPER {id: 11, features: [1.54, 0.3, 1.78]});
        CREATE (v3:PAPER {id: 12, features: [0.5, 1, 4.5]});
        CREATE (v4:PAPER {id: 13, features: [0.78, 0.234, 1.2]});
        MATCH (v1:PAPER {id: 10}), (v2:PAPER {id: 11}) CREATE (v1)-[e:CITES {}]->(v2);
        MATCH (v2:PAPER {id: 11}), (v3:PAPER {id: 12}) CREATE (v2)-[e:CITES {}]->(v3);
        MATCH (v3:PAPER {id: 12}), (v4:PAPER {id: 13}) CREATE (v3)-[e:CITES {}]->(v4);
        MATCH (v4:PAPER {id: 13}), (v1:PAPER {id: 10}) CREATE (v4)-[e:CITES {}]->(v1);
cleanup: |-
        CALL link_prediction.reset_parameters() YIELD *;
