setup: |-
    CALL node2vec_online.set_streamwalk_updater(7200, 2, 0.9, 604800, 2, False) YIELD *;
    CALL node2vec_online.set_word2vec_learner(2,0.01,True,1) YIELD *;
    CREATE TRIGGER update_embeddings ON --> CREATE BEFORE COMMIT EXECUTE CALL node2vec_online.update(createdEdges) YIELD *;
queries:
    - |-
        MERGE (a:Node {id: 0});
        MERGE (a:Node {id: 1});
        MERGE (a:Node {id: 2});
        MERGE (a:Node {id: 3});
        MERGE (b:Node {id: 4});
        MERGE (b:Node {id: 5});
    - |-
        MERGE (a:Node {id: 6});

cleanup: |-
    CALL node2vec_online.reset() YIELD *;
    DROP TRIGGER update_embeddings;
