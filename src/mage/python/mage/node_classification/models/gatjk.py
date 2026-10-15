import mgp
import torch
import torch.nn.functional as F
from torch_geometric.nn import GATConv, JumpingKnowledge, Linear


class GATJK(torch.nn.Module):
    def __init__(
        self,
        in_channels: int,
        hidden_features_size: mgp.List[int],
        out_channels: int,
        heads: int = 3,
        dropout: float = 0.6,
        jk_type: str = "max",
    ):
        """Initialization of model.

        Args:
            in_channels (int): dimension of input channels
            hidden_features_size (mgp.List[int]): list of dimensions of every hidden channel
            out_channels (int): dimension of output channels
            heads (int): number of heads for multi-head attention used for regularisation (https://petar-v.com/GAT/)
            dropout (float): ratio of layer outputs which are randomly ignored during training
            jk_type (str): type of aggregation mechanism for Jumping Knowledge Network as in
                            Representation Learning on Graphs with Jumping Knowledge Networks by K. Xu et al.
        """

        super(GATJK, self).__init__()

        self.convs = torch.nn.ModuleList()
        self.convs.append(
            GATConv(
                in_channels,
                hidden_features_size[0],
                heads=heads,
                concat=True,
                add_self_loops=False,
            )
        )

        self.bns = torch.nn.ModuleList()
        self.bns.append(torch.nn.BatchNorm1d(hidden_features_size[0] * heads))
        for i in range(len(hidden_features_size) - 2):
            self.convs.append(
                GATConv(
                    hidden_features_size[i] * heads,
                    hidden_features_size[i + 1],
                    heads=heads,
                    concat=True,
                    add_self_loops=False,
                )
            )
            self.bns.append(torch.nn.BatchNorm1d(hidden_features_size[i + 1] * heads))

        self.convs.append(
            GATConv(
                hidden_features_size[-2] * heads,
                hidden_features_size[-1],
                heads=heads,
                add_self_loops=False,
            )
        )

        self.dropout = dropout
        self.activation = F.elu  # note: uses elu

        self.jump = JumpingKnowledge(jk_type, channels=hidden_features_size[-1] * heads, num_layers=1)
        if jk_type == "cat":
            self.final_project = Linear(
                hidden_features_size[-1] * heads * len(hidden_features_size),
                out_channels,
            )
        else:  # max or lstm
            self.final_project = Linear(hidden_features_size[-1] * heads, out_channels)

    def reset_parameters(self):
        """Reset of parameters."""
        for conv in self.convs:
            conv.reset_parameters()
        for bn in self.bns:
            bn.reset_parameters()
        self.jump.reset_parameters()
        self.final_project.reset_parameters()

    def forward(self, x: torch.tensor, edge_index: torch.tensor) -> torch.tensor:
        """Forward passing of GATJK model.

        Args:
            x (torch.tensor): input features
            edge_index (torch.tensor): edge indices

        Returns:
            torch.tensor: embeddings after last layer of network is applied
        """
        xs = []
        for i, conv in enumerate(self.convs[:-1]):
            x = conv(x, edge_index)
            x = self.bns[i](x)
            x = self.activation(x)
            xs.append(x)
            x = F.dropout(x, p=self.dropout, training=self.training)
        x = self.convs[-1](x, edge_index)
        xs.append(x)
        x = self.jump(xs)
        x = self.final_project(x)
        return x
