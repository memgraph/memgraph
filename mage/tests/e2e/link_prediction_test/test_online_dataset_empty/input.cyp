queries:
    - |-
        RETURN 1;
cleanup: |-
    CALL link_prediction.reset_parameters() YIELD *;
