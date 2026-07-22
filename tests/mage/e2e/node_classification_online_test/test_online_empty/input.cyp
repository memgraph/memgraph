setup: |-
    CALL node_classification.reset() YIELD *;
    CALL node_classification.set_model_parameters() YIELD *;

queries: |-


cleanup: |-
    CALL node_classification.reset() YIELD *;
