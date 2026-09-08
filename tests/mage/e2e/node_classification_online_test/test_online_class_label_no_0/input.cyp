setup: |-
    CALL node_classification.set_model_parameters({layer_type: "GAT", learning_rate: 0.001, hidden_features_size: [2,2], class_name: "label", features_name: "features"}) YIELD *
queries:
    - |-
        CREATE (v1:PAPER {id: 10, features: [1, 2, 3], label:2});
        CREATE (v2:PAPER {id: 11, features: [1.54, 0.3, 1.78], label:2});
        CREATE (v3:PAPER {id: 12, features: [0.5, 1, 4.5], label:2});
        CREATE (v4:PAPER {id: 13, features: [0.78, 0.234, 1.2], label:2});
        CREATE (v5:PAPER {id: 14, features: [3, 4, 100], label:2});
        CREATE (v6:PAPER {id: 15, features: [2.1, 2.2, 2.3], label:1});
        CREATE (v7:PAPER {id: 16, features: [2.2, 2.3, 2.4], label:1});
        CREATE (v8:PAPER {id: 17, features: [2.3, 2.4, 2.5], label:1});
        CREATE (v9:PAPER {id: 18, features: [2.4, 2.5, 2.6], label:1});
        MATCH (v1:PAPER {id:10}), (v2:PAPER {id:11}) CREATE (v1)-[e:CITES {}]->(v2);
        MATCH (v2:PAPER {id:11}), (v3:PAPER {id:12}) CREATE (v2)-[e:CITES {}]->(v3);
        MATCH (v3:PAPER {id:12}), (v4:PAPER {id:13}) CREATE (v3)-[e:CITES {}]->(v4);
        MATCH (v4:PAPER {id:13}), (v1:PAPER {id:10}) CREATE (v4)-[e:CITES {}]->(v1);
        MATCH (v4:PAPER {id:13}), (v5:PAPER {id:14}) CREATE (v4)-[e:CITES {}]->(v5);
        MATCH (v5:PAPER {id:14}), (v6:PAPER {id:15}) CREATE (v5)-[e:CITES {}]->(v6);
        MATCH (v6:PAPER {id:15}), (v7:PAPER {id:16}) CREATE (v6)-[e:CITES {}]->(v7);
        MATCH (v7:PAPER {id:16}), (v8:PAPER {id:17}) CREATE (v7)-[e:CITES {}]->(v8);
        MATCH (v8:PAPER {id:17}), (v9:PAPER {id:18}) CREATE (v8)-[e:CITES {}]->(v9);
        MATCH (v9:PAPER {id:18}), (v6:PAPER {id:15}) CREATE (v9)-[e:CITES {}]->(v6);
    - |-
        CREATE (v10:PAPER {id: 19, features: [1, 4, 3], label:2});
        MATCH (v1:PAPER {id:10}), (v10:PAPER {id:19}) CREATE (v1)-[e:CITES {}]->(v10);
        MATCH (v2:PAPER {id:11}), (v10:PAPER {id:19}) CREATE (v10)-[e:CITES {}]->(v2);


cleanup: |-
        CALL node_classification.reset() YIELD *;
