from enum import Enum, EnumMeta


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class BaseEnum(str, Enum, metaclass=MetaEnum):
    pass


class Metrics(BaseEnum):
    LOSS = "loss"
    ACCURACY = "accuracy"
    AUC_SCORE = "auc_score"
    PRECISION = "precision"
    RECALL = "recall"
    F1 = "f1"
    POS_EXAMPLES = "pos_examples"
    NEG_EXAMPLES = "neg_examples"
    POS_PRED_EXAMPLES = "pos_pred_examples"
    NEG_PRED_EXAMPLES = "neg_pred_examples"
    EPOCH = "epoch"
    TRUE_POSITIVES = "true_positives"
    FALSE_POSITIVES = "false_positives"
    TRUE_NEGATIVES = "true_negatives"
    FALSE_NEGATIVES = "false_negatives"


class Predictors(BaseEnum):
    NODE_EMBEDDINGS = "node_embeddings"
    EDGE_SCORE = "edge_score"
    DOT_PREDICTOR = "dot"
    MLP_PREDICTOR = "mlp"


class Reindex(BaseEnum):
    DGL = "dgl"  # DGL to Memgraph indexes
    MEMGRAPH = "memgraph"  # Memgraph to DGL indexes


class Context(BaseEnum):
    MODEL_NAME = "model.pt"
    PREDICTOR_NAME = "predictor.pt"


class Models(BaseEnum):
    GRAPH_SAGE = "graph_sage"
    GRAPH_ATTN = "graph_attn"


class Optimizers(BaseEnum):
    ADAM_OPT = "ADAM"
    SGD_OPT = "SGD"


class Devices(BaseEnum):
    CUDA_DEVICE = "cuda"
    CPU_DEVICE = "cpu"


class Aggregators(BaseEnum):
    MEAN_AGG = "mean"
    LSTM_AGG = "lstm"
    POOL_AGG = "pool"
    GCN_AGG = "gcn"


class Activations(BaseEnum):
    SIGMOID = "sigmoid"


class Parameters(BaseEnum):
    HIDDEN_FEATURES_SIZE = "hidden_features_size"
    LAYER_TYPE = "layer_type"
    NUM_EPOCHS = "num_epochs"
    OPTIMIZER = "optimizer"
    LEARNING_RATE = "learning_rate"
    SPLIT_RATIO = "split_ratio"
    NODE_FEATURES_PROPERTY = "node_features_property"
    DEVICE_TYPE = "device_type"
    CONSOLE_LOG_FREQ = "console_log_freq"
    CHECKPOINT_FREQ = "checkpoint_freq"
    AGGREGATOR = "aggregator"
    METRICS = "metrics"
    PREDICTOR_TYPE = "predictor_type"
    ATTN_NUM_HEADS = "attn_num_heads"
    TR_ACC_PATIENCE = "tr_acc_patience"
    MODEL_SAVE_PATH = "model_save_path"
    CONTEXT_SAVE_DIR = "context_save_dir"
    TARGET_RELATION = "target_relation"
    NUM_NEG_PER_POS_EDGE = "num_neg_per_pos_edge"
    BATCH_SIZE = "batch_size"
    SAMPLING_WORKERS = "sampling_workers"
    NUM_LAYERS = "num_layers"
    DROPOUT = "dropout"
    RESIDUAL = "residual"
    ALPHA = "alpha"
    LAST_ACTIVATION_FUNCTION = "last_activation_function"
    ADD_REVERSE_EDGES = "add_reverse_edges"
    ADD_SELF_LOOPS = "add_self_loops"
