queries:
    - |-
        CALL node2vec_online.set_word2vec_learner(2,0.01,True,1) YIELD *;
    - |-
        CALL node2vec_online.reset() YIELD *;
