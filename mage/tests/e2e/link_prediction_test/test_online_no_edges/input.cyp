queries:
    - |-
        MERGE (a:PAPER {id: 1});
        MERGE (a:PAPER {id: 2});
        MERGE (a:PAPER {id: 3});
        MERGE (a:PAPER {id: 4});
        MERGE (a:PAPER {id: 5});

cleanup: |-
    CALL link_prediction.reset_parameters() YIELD *;
