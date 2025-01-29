queries:
    - |-
        CALL node2vec_online.set_streamwalk_updater(7200, 2, 0.9, 604800, 2, False) YIELD *;
    - |-
        CALL node2vec_online.reset() YIELD *;
