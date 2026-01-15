from typing import Dict, List

import dgl
import torch
from dgl.nn import HeteroGraphConv
from dgl.nn.pytorch import GATConv


class GAT(torch.nn.Module):
    def __init__(
        self,
        in_feats: int,
        hidden_features_size: List[int],
        attn_num_heads: List[int],
        feat_drops: List[float],
        attn_drops: List[float],
        alphas: List[float],
        residuals: List[bool],
        edge_types: List[str],
        device: torch.device,
    ):
        """Initializes GAT module with layer sizes.

        Args:
            in_feats (int): Defines the size of the input features.
            hidden_features_size (List[int]): First element is the feature size and the rest specifies layer size.
            attn_num_heads List[int]: List of number of heads for each layer. Last layer must have only one head.
            feat_drops List[float]: Dropout rate on feature for each layer.
            attn_drops List[float]: Dropout rate on attention weights for each layer.
            alphas List[float]: LeakyReLU angle of negative slope for each layer.
            residuals List[bool]: Use residual connection for each layer or not.
            edge_types (List[str]): All edge types that are occurring in the heterogeneous network.
        """
        super(GAT, self).__init__()
        self.layers = torch.nn.ModuleList()
        self.num_layers = len(hidden_features_size)
        # Define activations
        activations = [torch.nn.functional.elu for _ in range(self.num_layers - 1)]  # All activations except last layer
        activations.append(None)
        # Iterate through all layers
        for i in range(self.num_layers):
            gat_layer = GATConv(
                in_feats=in_feats,
                out_feats=hidden_features_size[i],
                num_heads=attn_num_heads[i],
                feat_drop=feat_drops[i],
                attn_drop=attn_drops[i],
                negative_slope=alphas[i],
                residual=residuals[i],
                activation=activations[i],
                allow_zero_in_degree=True,
            ).to(device)

            self.layers.append(
                HeteroGraphConv({edge_type: gat_layer for edge_type in edge_types}, aggregate="sum").to(device)
            )
            in_feats = hidden_features_size[i]

    def forward(self, blocks: List[dgl.graph], h: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        """Performs forward pass on batches.

        Args:
            blocks (List[dgl.heterograph.DGLBlock]): First block is DGLBlock of all nodes that are needed to compute representations for second block. Second block is sampled graph.
            h (Dict[str, torch.Tensor]): Input features for every node type.

        Returns:
            Dict[str, torch.Tensor]: Embeddings for every node type.
        """
        for index, layer in enumerate(self.layers):
            h = layer(blocks[index], h)
            h = {k: torch.mean(v, dim=1) for k, v in h.items()}

        return h

    def online_forward(self, graph: dgl.graph, h: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        """Performs forward pass on batches.

        Args:
            graph (dgl.heterograph): Whole graph instance used in prediction.
            h (Dict[str, torch.Tensor]): Input features for every node type.

        Returns:
            Dict[str, torch.Tensor]: Embeddings for every node type.
        """
        for layer in self.layers:
            h = layer(graph, h)
            h = {k: torch.mean(v, dim=1) for k, v in h.items()}

        return h
