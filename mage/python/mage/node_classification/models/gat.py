import mgp
from mage.node_classification.models.inductive_model import InductiveModel


class GAT(InductiveModel):
    def __init__(
        self,
        in_channels: int,
        hidden_features_size: mgp.List[int],
        out_channels: int,
        aggr: str,
    ):
        super().__init__(
            layer_type="GAT",
            in_channels=in_channels,
            hidden_features_size=hidden_features_size,
            out_channels=out_channels,
            aggr=aggr,
        )
