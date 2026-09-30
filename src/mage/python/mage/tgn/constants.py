import enum


class TGNLayerType(enum.Enum):
    GraphSumEmbedding = "graph_sum"
    GraphAttentionEmbedding = "graph_attn"


class MessageFunctionType(enum.Enum):
    MLP = "mlp"
    Identity = "identity"


class MemoryUpdaterType(enum.Enum):
    GRU = "gru"
    RNN = "rnn"


class MessageAggregatorType(enum.Enum):
    Mean = "mean"
    Last = "last"
