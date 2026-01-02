setup: |-
    CALL tgn.set_params({learning_type:'self_supervised', batch_size:5, num_of_layers:2, layer_type:'graph_attn', memory_dimension:100, time_dimension:100, num_edge_features:7, num_node_features:10, message_dimension:100, num_neighbors:5, edge_message_function_type:'identity',message_aggregator_type:'last', memory_updater_type:'gru', num_attention_heads:1});
    CREATE TRIGGER create_embeddings ON --> CREATE BEFORE COMMIT EXECUTE CALL tgn.update(createdEdges) YIELD *;


queries:
    - |-
        MERGE (a:User {id: 2, features:[0.99,0.96,0.34,0.38,0.20,0.37,0.14,0.01,0.03,0.32]}) MERGE (b:Item {id: 2, features:[0.99,0.96,0.34,0.38,0.20,0.37,0.14,0.01,0.03,0.32]}) CREATE (a)-[:CLICKED {features:[-0.18,-0.18,-0.94,-0.38,0.00,-0.64,1.05]}]->(b);
        MERGE (a:User {id: 2, features:[0.99,0.96,0.34,0.38,0.20,0.37,0.14,0.01,0.03,0.32]}) MERGE (b:Item {id: 2, features:[0.99,0.96,0.34,0.38,0.20,0.37,0.14,0.01,0.03,0.32]}) CREATE (a)-[:CLICKED {features:[-0.18,-0.18,-0.94,-0.38,0.00,-0.64,1.05]}]->(b);
        MERGE (a:User {id: 3, features:[0.09,0.23,0.83,0.06,0.66,0.84,0.26,0.73,0.57,0.80]}) MERGE (b:Item {id: 3, features:[0.09,0.23,0.83,0.06,0.66,0.84,0.26,0.73,0.57,0.80]}) CREATE (a)-[:CLICKED {features:[-0.18,-0.18,-0.94,-0.38,0.00,-0.64,1.05]}]->(b);
        MERGE (a:User {id: 2, features:[0.99,0.96,0.34,0.38,0.20,0.37,0.14,0.01,0.03,0.32]}) MERGE (b:Item {id: 2, features:[0.99,0.96,0.34,0.38,0.20,0.37,0.14,0.01,0.03,0.32]}) CREATE (a)-[:CLICKED {features:[-0.18,-0.18,-0.94,-0.38,0.00,-0.64,1.05]}]->(b);
        MERGE (a:User {id: 3, features:[0.09,0.23,0.83,0.06,0.66,0.84,0.26,0.73,0.57,0.80]}) MERGE (b:Item {id: 3, features:[0.09,0.23,0.83,0.06,0.66,0.84,0.26,0.73,0.57,0.80]}) CREATE (a)-[:CLICKED {features:[-0.18,-0.18,-0.94,-0.38,0.00,-0.64,1.05]}]->(b);
    - |-
        MERGE (a:User {id: 4, features:[0.81,0.03,0.64,0.57,0.16,0.95,0.65,0.97,0.95,0.98]}) MERGE (b:Item {id: 4, features:[0.81,0.03,0.64,0.57,0.16,0.95,0.65,0.97,0.95,0.98]}) CREATE (a)-[:CLICKED {features:[-0.09,-0.09,0.93,0.20,0.00,0.00,-0.90]}]->(b);
        MERGE (a:User {id: 2, features:[0.99,0.96,0.34,0.38,0.20,0.37,0.14,0.01,0.03,0.32]}) MERGE (b:Item {id: 2, features:[0.99,0.96,0.34,0.38,0.20,0.37,0.14,0.01,0.03,0.32]}) CREATE (a)-[:CLICKED {features:[-0.18,-0.18,-0.94,-0.38,0.00,-0.64,1.05]}]->(b);
        MERGE (a:User {id: 5, features:[0.06,0.60,0.70,0.16,0.61,0.51,0.84,0.53,0.93,0.92]}) MERGE (b:Item {id: 5, features:[0.06,0.60,0.70,0.16,0.61,0.51,0.84,0.53,0.93,0.92]}) CREATE (a)-[:CLICKED {features:[0.16,0.16,0.75,0.66,0.00,1.49,-1.08]}]->(b);
        MERGE (a:User {id: 5, features:[0.06,0.60,0.70,0.16,0.61,0.51,0.84,0.53,0.93,0.92]}) MERGE (b:Item {id: 5, features:[0.06,0.60,0.70,0.16,0.61,0.51,0.84,0.53,0.93,0.92]}) CREATE (a)-[:CLICKED {features:[-0.16,-0.15,1.26,-0.38,0.00,-0.64,-0.91]}]->(b);
        MERGE (a:User {id: 6, features:[0.68,0.79,0.18,0.06,0.46,0.09,0.69,0.43,0.41,0.43]}) MERGE (b:Item {id: 6, features:[0.68,0.79,0.18,0.06,0.46,0.09,0.69,0.43,0.41,0.43]}) CREATE (a)-[:CLICKED {features:[-0.10,-0.11,1.29,-0.38,0.00,0.88,-1.11]}]->(b);
cleanup: |-
    CALL tgn.reset() YIELD *;
    DROP TRIGGER create_embeddings;
