from typing import Dict, Tuple

import dgl
import torch
import torch.nn.functional as F
from mage.link_prediction.constants import Predictors


class MLPPredictor(torch.nn.Module):
    def __init__(self, h_feats: int, device: torch.device) -> None:
        super().__init__()

        self.W1 = torch.nn.Linear(h_feats * 2, h_feats, device=device)
        self.W2 = torch.nn.Linear(h_feats, 1, device=device)

    def apply_edges(self, edges: Tuple[torch.Tensor, torch.Tensor]) -> Dict:
        """Computes a scalar score for each edge of the given graph.

        Args:
            edges (Tuple[torch.Tensor, torch.Tensor]): Has three members: ``src``, ``dst`` and ``data``, each of which is a dictionary representing the features of the source nodes, the destination nodes and the edges themselves.
        Returns:
            Dict: A dictionary of new edge features
        """
        h = torch.cat(
            [
                edges.src[Predictors.NODE_EMBEDDINGS],
                edges.dst[Predictors.NODE_EMBEDDINGS],
            ],
            1,
        )
        return {Predictors.EDGE_SCORE: self.W2(F.relu(self.W1(h))).squeeze(1)}

    def forward(
        self,
        g: dgl.graph,
        node_embeddings: Dict[str, torch.Tensor],
        target_relation: str = None,
    ) -> torch.Tensor:
        """Calculates forward pass of MLPPredictor.

        Args:
            g (dgl.graph): A reference to the graph for which edge scores will be computed.
            node_embeddings (Dict[str, torch.Tensor]): node embeddings for each node type.
            target_relation: str -> Unique edge type that is used for training.
        Returns:
            torch.Tensor: A tensor of edge scores.
        """
        with g.local_scope():
            for node_type in node_embeddings.keys():  # Iterate over all node_types.
                g.nodes[node_type].data[Predictors.NODE_EMBEDDINGS] = node_embeddings[node_type]

            g.apply_edges(self.apply_edges, etype=target_relation)
            scores = g.edata[Predictors.EDGE_SCORE]

            if not isinstance(scores, dict):
                return scores.view(-1)

            if isinstance(target_relation, tuple):  # Tuple[str, str, str] identification
                return scores[target_relation].view(-1)

            if isinstance(target_relation, str):
                for key, val in scores.items():
                    if key[1] == target_relation:
                        return val.view(-1)

    def forward_pred(self, src_embedding: torch.Tensor, dest_embedding: torch.Tensor) -> float:
        """Efficient implementation for predict method of DotPredictor.

        Args:
            src_embedding (torch.Tensor): Embedding of the source node.
            dest_embedding (torch.Tensor): Embedding of the destination node.

        Returns:
            float: Edge score computed.
        """
        h = torch.cat([src_embedding, dest_embedding])
        return self.W2(F.relu(self.W1(h)))
