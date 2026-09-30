"""
Instances of TGN
"""
from typing import Dict, Tuple

import numpy as np
import torch
import torch.nn as nn
from mage.tgn.constants import MemoryUpdaterType, MessageAggregatorType, MessageFunctionType, TGNLayerType
from mage.tgn.definitions.layers import TGNLayerGraphAttentionEmbedding, TGNLayerGraphSumEmbedding
from mage.tgn.definitions.tgn import TGN


class TGNEdgesSelfSupervised(TGN):
    """
    Class contains forward method different for edges self_supervised learning type
    """

    def forward(
        self,
        data: Tuple[
            np.ndarray,
            np.ndarray,
            np.ndarray,
            np.ndarray,
            np.ndarray,
            np.ndarray,
            Dict[int, torch.Tensor],
            Dict[int, torch.Tensor],
        ],
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        :param data: input containing sources, destinations, negative_sources,  negative_destinations,
            timestamps, edge_idxs, edge_features, node_features
        :return embeddings for sources and destinations in first result, embeddings for negative sources and destinations
        as other result
        """
        (
            sources,
            destinations,
            negative_sources,
            negative_destinations,
            timestamps,
            edge_idxs,
            edge_features,
            node_features,
        ) = data

        assert (
            sources.shape[0]
            == destinations.shape[0]
            == negative_sources.shape[0]
            == negative_destinations.shape[0]
            == timestamps.shape[0]
            == len(edge_idxs)
            == len(edge_features)
        ), (
            f"Sources, destinations, negative sources, negative destinations, timestamps, edge_indexes and edge_features must be of same dimension, but got "
            f"{sources.shape[0]}, {destinations.shape[0]}, {timestamps.shape[0]}, {len(edge_idxs)}, {len(edge_features)}"
        )

        # part of 1->2->3, all till point 4 in paper from Figure 2
        # we are using this part so that we can get gradients from memory module also and so that they
        # can be included in optimizer
        # By doing this, the computation of the memory-related modules directly influences the loss
        self._process_previous_batches()

        graph_data = self._get_graph_data(
            np.concatenate([sources.copy(), destinations.copy()], dtype=int),
            np.concatenate([timestamps, timestamps]),
        )

        embeddings = self.tgn_net(graph_data)

        graph_data_negative = self._get_graph_data(
            np.concatenate([negative_sources.copy(), negative_destinations.copy()], dtype=int),
            np.concatenate([timestamps, timestamps]),
        )

        embeddings_negative = self.tgn_net(graph_data_negative)

        # here we update raw message store, and this batch will be used in next
        # call of tgn in function self.process_previous_batches
        # the raw messages for this batch interactions are stored in the raw
        # message store  to be used in future batches.
        # in paper on figure 2 this is part 7.
        self._process_current_batch(sources, destinations, node_features, edge_features, edge_idxs, timestamps)

        return embeddings, embeddings_negative


class TGNSupervised(TGN):
    """
    Class contains forward method different for supervised learning type
    """

    def forward(
        self,
        data: Tuple[
            np.ndarray,
            np.ndarray,
            np.ndarray,
            np.ndarray,
            Dict[int, torch.Tensor],
            Dict[int, torch.Tensor],
        ],
    ) -> torch.Tensor:
        """
        :param data: input containing sources, destinations,
            timestamps, edge_idxs, edge_features, node_features
        :return embeddings for sources and destinations in one torch.Tensor
        """

        (
            sources,
            destinations,
            timestamps,
            edge_idxs,
            edge_features,
            node_features,
        ) = data

        assert (
            sources.shape[0] == destinations.shape[0] == timestamps.shape[0] == len(edge_idxs) == len(edge_features)
        ), (
            f"Sources, destinations, timestamps, edge_indexes and edge_features must be of same dimension, but got "
            f"{sources.shape[0]}, {destinations.shape[0]}, {timestamps.shape[0]}, {len(edge_idxs)}, {len(edge_features)}"
        )

        # part of 1->2->3, all till point 4 in paper from Figure 2
        # we are using this part so that we can get gradients from memory module also and so that they
        # can be included in optimizer
        # By doing this, the computation of the memory-related modules directly influences the loss
        self._process_previous_batches()

        graph_data = self._get_graph_data(
            np.concatenate([sources.copy(), destinations.copy()], dtype=int),
            np.concatenate([timestamps, timestamps]),
        )

        embeddings = self.tgn_net(graph_data)

        # here we update raw message store, and this batch will be used in next
        # call of tgn in function self.process_previous_batches
        # the raw messages for this batch interactions are stored in the raw
        # message store  to be used in future batches.
        # in paper on figure 2 this is part 7.
        self._process_current_batch(sources, destinations, node_features, edge_features, edge_idxs, timestamps)

        return embeddings


class TGNGraphAttentionEmbedding(TGN):
    def __init__(
        self,
        num_of_layers: int,
        layer_type: TGNLayerType,
        memory_dimension: int,
        time_dimension: int,
        num_edge_features: int,
        num_node_features: int,
        message_dimension: int,
        num_neighbors: int,
        edge_message_function_type: MessageFunctionType,
        message_aggregator_type: MessageAggregatorType,
        memory_updater_type: MemoryUpdaterType,
        num_attention_heads: int,
        device: torch.device,
    ):
        super().__init__(
            num_of_layers,
            layer_type,
            memory_dimension,
            time_dimension,
            num_edge_features,
            num_node_features,
            message_dimension,
            num_neighbors,
            edge_message_function_type,
            message_aggregator_type,
            memory_updater_type,
            device,
        )

        assert layer_type == TGNLayerType.GraphAttentionEmbedding

        self.num_attention_heads = num_attention_heads

        # Initialize TGN layers
        self.tgn_layers = []

        layer = TGNLayerGraphAttentionEmbedding(
            embedding_dimension=self.memory_dimension + self.num_node_features,
            edge_feature_dim=self.num_edge_features,
            time_encoding_dim=self.time_dimension,
            node_features_dim=self.num_node_features,
            num_neighbors=self.num_neighbors,
            num_attention_heads=num_attention_heads,
            num_of_layers=self.num_of_layers,
            device=self.device,
        )

        self.tgn_layers.append(layer)

        self.tgn_net = nn.Sequential(*self.tgn_layers)


class TGNGraphSumEmbedding(TGN):
    def __init__(
        self,
        num_of_layers: int,
        layer_type: TGNLayerType,
        memory_dimension: int,
        time_dimension: int,
        num_edge_features: int,
        num_node_features: int,
        message_dimension: int,
        num_neighbors: int,
        edge_message_function_type: MessageFunctionType,
        message_aggregator_type: MessageAggregatorType,
        memory_updater_type: MemoryUpdaterType,
        device: torch.device,
    ):
        super().__init__(
            num_of_layers,
            layer_type,
            memory_dimension,
            time_dimension,
            num_edge_features,
            num_node_features,
            message_dimension,
            num_neighbors,
            edge_message_function_type,
            message_aggregator_type,
            memory_updater_type,
            device,
        )

        assert layer_type == TGNLayerType.GraphSumEmbedding

        # Initialize TGN layers
        self.tgn_layers = []

        layer = TGNLayerGraphSumEmbedding(
            embedding_dimension=self.memory_dimension + self.num_node_features,
            edge_feature_dim=self.num_edge_features,
            time_encoding_dim=self.time_dimension,
            node_features_dim=self.num_node_features,
            num_neighbors=self.num_neighbors,
            num_of_layers=self.num_of_layers,
            device=self.device,
        )

        self.tgn_layers.append(layer)

        self.tgn_net = nn.Sequential(*self.tgn_layers)


#
# TGN instances - [Supervised, Self_supervised] x [Graph_attention, Graph_sum]
#


# Self_supervised x Graph_attention
class TGNGraphAttentionEdgeSelfSupervised(TGNGraphAttentionEmbedding, TGNEdgesSelfSupervised):
    def __init__(
        self,
        num_of_layers: int,
        layer_type: TGNLayerType,
        memory_dimension: int,
        time_dimension: int,
        num_edge_features: int,
        num_node_features: int,
        message_dimension: int,
        num_neighbors: int,
        edge_message_function_type: MessageFunctionType,
        message_aggregator_type: MessageAggregatorType,
        memory_updater_type: MemoryUpdaterType,
        num_attention_heads: int,
        device: torch.device,
    ):
        super().__init__(
            num_of_layers,
            layer_type,
            memory_dimension,
            time_dimension,
            num_edge_features,
            num_node_features,
            message_dimension,
            num_neighbors,
            edge_message_function_type,
            message_aggregator_type,
            memory_updater_type,
            num_attention_heads,
            device,
        )


# Self_supervised x Graph_sum
class TGNGraphSumEdgeSelfSupervised(TGNGraphSumEmbedding, TGNEdgesSelfSupervised):
    def __init__(
        self,
        num_of_layers: int,
        layer_type: TGNLayerType,
        memory_dimension: int,
        time_dimension: int,
        num_edge_features: int,
        num_node_features: int,
        message_dimension: int,
        num_neighbors: int,
        edge_message_function_type: MessageFunctionType,
        message_aggregator_type: MessageAggregatorType,
        memory_updater_type: MemoryUpdaterType,
        device: torch.device,
    ):
        super().__init__(
            num_of_layers,
            layer_type,
            memory_dimension,
            time_dimension,
            num_edge_features,
            num_node_features,
            message_dimension,
            num_neighbors,
            edge_message_function_type,
            message_aggregator_type,
            memory_updater_type,
            device,
        )


# Supervised x Graph_sum
class TGNGraphSumSupervised(TGNGraphSumEmbedding, TGNSupervised):
    def __init__(
        self,
        num_of_layers: int,
        layer_type: TGNLayerType,
        memory_dimension: int,
        time_dimension: int,
        num_edge_features: int,
        num_node_features: int,
        message_dimension: int,
        num_neighbors: int,
        edge_message_function_type: MessageFunctionType,
        message_aggregator_type: MessageAggregatorType,
        memory_updater_type: MemoryUpdaterType,
        device: torch.device,
    ):
        super().__init__(
            num_of_layers,
            layer_type,
            memory_dimension,
            time_dimension,
            num_edge_features,
            num_node_features,
            message_dimension,
            num_neighbors,
            edge_message_function_type,
            message_aggregator_type,
            memory_updater_type,
            device,
        )


# Supervised x Graph_attention
class TGNGraphAttentionSupervised(TGNGraphAttentionEmbedding, TGNSupervised):
    def __init__(
        self,
        num_of_layers: int,
        layer_type: TGNLayerType,
        memory_dimension: int,
        time_dimension: int,
        num_edge_features: int,
        num_node_features: int,
        message_dimension: int,
        num_neighbors: int,
        edge_message_function_type: MessageFunctionType,
        message_aggregator_type: MessageAggregatorType,
        memory_updater_type: MemoryUpdaterType,
        num_attention_heads: int,
        device: torch.device,
    ):
        super().__init__(
            num_of_layers,
            layer_type,
            memory_dimension,
            time_dimension,
            num_edge_features,
            num_node_features,
            message_dimension,
            num_neighbors,
            edge_message_function_type,
            message_aggregator_type,
            memory_updater_type,
            num_attention_heads,
            device,
        )
