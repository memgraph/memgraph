from typing import Dict, List, Tuple, Union

import numpy as np
import torch
import torch.nn as nn
from mage.tgn.constants import MemoryUpdaterType, MessageAggregatorType, MessageFunctionType, TGNLayerType
from mage.tgn.definitions.events import Event, InteractionEvent, NodeEvent, create_interaction_events
from mage.tgn.definitions.memory import Memory
from mage.tgn.definitions.memory_updater import MemoryUpdater, MemoryUpdaterGRU, MemoryUpdaterRNN
from mage.tgn.definitions.message_aggregator import LastMessageAggregator, MeanMessageAggregator, MessageAggregator
from mage.tgn.definitions.message_function import MessageFunction, MessageFunctionIdentity, MessageFunctionMLP
from mage.tgn.definitions.messages import InteractionRawMessage, NodeRawMessage, RawMessage
from mage.tgn.definitions.raw_message_store import RawMessageStore
from mage.tgn.definitions.temporal_neighborhood import TemporalNeighborhood
from mage.tgn.definitions.time_encoding import TimeEncoder

GraphSumDataType = Tuple[
    List[List[Tuple[int, int]]],
    List[Dict[Tuple[int, int], int]],
    List[List[int]],
    List[List[Tuple[int, int]]],
    torch.Tensor,
    List[torch.Tensor],
    List[torch.Tensor],
]
GraphAttnDataType = Tuple[GraphSumDataType, torch.Tensor]
GraphDataType = Union[GraphSumDataType, GraphAttnDataType]


class TGN(nn.Module):
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
        super().__init__()
        self.num_of_layers = num_of_layers
        self.memory_dimension = memory_dimension
        self.time_dimension = time_dimension
        self.num_node_features = num_node_features
        self.num_edge_features = num_edge_features
        self.layer_type = layer_type
        self.device = device

        self.num_neighbors = num_neighbors

        self.edge_features: Dict[int, torch.Tensor] = {}
        self.node_features: Dict[int, torch.Tensor] = {}

        self.temporal_neighborhood = TemporalNeighborhood()

        # dimension of raw message for edge
        # m_ij = (s_i, s_j, delta t, e_ij)
        # delta t is only one number :)
        self.edge_raw_message_dimension = 2 * self.memory_dimension + 1 + num_edge_features

        # dimension of raw message for node
        # m_i = (s_i, t, v_i)
        self.node_raw_message_dimension = self.memory_dimension + 1 + num_node_features

        self.raw_message_store = RawMessageStore(
            edge_raw_message_dimension=self.edge_raw_message_dimension,
            node_raw_message_dimension=self.node_raw_message_dimension,
        )

        MessageFunctionEdge = get_message_function_type(edge_message_function_type)

        # if edge function is identity, when identity function is applied, it will result with
        # vector of greater dimension then when identity is applied to node raw message, so node function must be MLP
        # if edge is MLP, node also will be MLP
        MessageFunctionNode = MessageFunctionMLP

        self.message_dimension = message_dimension

        if edge_message_function_type == MessageFunctionType.Identity:
            # We set message dimension (dimension after message function is applied to raw message) to
            # dimension of raw message of edge interaction event, since message dimension will be
            # just dimension of concatenated vectors in edge interaction event.
            # Also, MLP in node event will then produce vector of same dimension and we can then aggregate these
            # two vectors, with MEAN or LAST
            self.message_dimension = self.edge_raw_message_dimension

        self.edge_message_function = MessageFunctionEdge(
            message_dimension=self.message_dimension,
            raw_message_dimension=self.edge_raw_message_dimension,
            device=self.device,
        )

        self.node_message_function = MessageFunctionNode(
            message_dimension=self.message_dimension,
            raw_message_dimension=self.node_raw_message_dimension,
            device=self.device,
        )

        MessageAggregator = get_message_aggregator_type(message_aggregator_type)

        self.message_aggregator = MessageAggregator()

        self.memory = Memory(memory_dimension=memory_dimension, device=self.device)

        MemoryUpdaterType = get_memory_updater_type(memory_updater_type)

        self.memory_updater = MemoryUpdaterType(
            memory_dimension=self.memory_dimension,
            message_dimension=self.message_dimension,
            device=self.device,
        )

        self.time_encoder = TimeEncoder(out_dimension=self.time_dimension, device=self.device)

    def detach_tensor_grads(self) -> None:
        self.memory.detach_tensor_grads()
        self.raw_message_store.detach_grads()

    def init_memory(self):
        self.memory.init_memory()

    def init_temporal_neighborhood(self):
        self.temporal_neighborhood.init_temporal_neighborhood()

    def init_message_store(self):
        self.raw_message_store.init_message_store()

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
    ):
        raise Exception("Not implemented")

    def _process_current_batch(
        self,
        sources: np.array,
        destinations: np.array,
        node_features: Dict[int, torch.Tensor],
        edge_features: Dict[int, torch.Tensor],
        edge_idxs: np.array,
        timestamps: np.array,
    ) -> None:
        self._update_raw_message_store_current_batch(
            sources=sources,
            destinations=destinations,
            node_features=node_features,
            edge_features=edge_features,
            edge_idxs=edge_idxs,
            timestamps=timestamps,
        )

        self.temporal_neighborhood.update_neighborhood(
            sources=sources,
            destinations=destinations,
            timestamps=timestamps,
            edge_idxs=edge_idxs,
        )

        for edge_idx, edge_feature in edge_features.items():
            self.edge_features[edge_idx] = edge_feature

        for node_id, node_feature in node_features.items():
            self.node_features[node_id] = node_feature

    def _process_previous_batches(self) -> None:
        # dict nodeid -> List[event]
        raw_messages = self.raw_message_store.get_messages()

        processed_messages = self._create_messages(
            raw_messages=raw_messages,
        )

        aggregated_messages = self._aggregate_messages(
            processed_messages=processed_messages,
        )

        self._update_memory(aggregated_messages)

    def _update_raw_message_store_current_batch(
        self,
        sources: np.array,
        destinations: np.array,
        timestamps: np.array,
        edge_idxs: np.array,
        edge_features: Dict[int, torch.Tensor],
        node_features: Dict[int, torch.Tensor],
    ) -> None:
        interaction_events: Dict[int, List[Event]] = create_interaction_events(
            sources=sources,
            destinations=destinations,
            timestamps=timestamps,
            edge_idxs=edge_idxs,
        )

        # this is what TGN gets
        events: Dict[int, List[Event]] = interaction_events

        raw_messages: Dict[int, List[RawMessage]] = self._create_raw_messages(
            events=events,
            edge_features=edge_features,
            node_features=node_features,
        )

        self.raw_message_store.update_messages(raw_messages)

    def _create_node_events(
        self,
    ):
        raise NotImplementedError()

    def _create_messages(
        self,
        raw_messages: Dict[int, List[RawMessage]],
    ) -> Dict[int, List[torch.Tensor]]:
        processed_messages_dict = {node: [] for node in raw_messages}
        for node in raw_messages:
            for message in raw_messages[node]:
                if type(message) is NodeRawMessage:
                    node_raw_message = message
                    processed_messages_dict[node].append(
                        self.node_message_function(
                            (
                                node_raw_message.source_memory,
                                node_raw_message.timestamp,
                                node_raw_message.node_features,
                            )
                        )
                    )
                elif type(message) is InteractionRawMessage:
                    interaction_raw_message = message

                    processed_messages_dict[node].append(
                        self.edge_message_function(
                            (
                                interaction_raw_message.source_memory,
                                interaction_raw_message.dest_memory,
                                interaction_raw_message.delta_time,
                                interaction_raw_message.edge_features,
                            )
                        )
                    )
                else:
                    raise Exception(f"Message type not supported {type(message)}")
        return processed_messages_dict

    def _create_raw_messages(
        self,
        events: Dict[int, List[Event]],
        node_features: Dict[int, torch.Tensor],
        edge_features: Dict[int, torch.Tensor],
    ) -> Dict[int, List[RawMessage]]:
        raw_messages = {node: [] for node in events}
        for node in events:
            for event in events[node]:
                assert node == event.source
                if type(event) is NodeEvent:
                    raw_messages[node].append(
                        NodeRawMessage(
                            source_memory=self.memory.get_node_memory(node),
                            timestamp=event.timestamp,
                            node_features=node_features[node],
                            source=node,
                        )
                    )
                elif type(event) is InteractionEvent:
                    # every interaction event creates two raw messages
                    raw_messages[event.source].append(
                        InteractionRawMessage(
                            source_memory=self.memory.get_node_memory(event.source),
                            timestamp=event.timestamp,
                            dest_memory=self.memory.get_node_memory(event.dest),
                            source=node,
                            edge_features=edge_features[event.edge_idx],
                            delta_time=torch.tensor(
                                np.array(event.timestamp).astype("float"),
                                requires_grad=True,
                                device=self.device,
                            )
                            - self.memory.get_last_node_update(event.source),
                        )
                    )
                    raw_messages[event.dest].append(
                        InteractionRawMessage(
                            source_memory=self.memory.get_node_memory(event.dest),
                            timestamp=event.timestamp,
                            dest_memory=self.memory.get_node_memory(event.source),
                            source=event.dest,
                            edge_features=edge_features[event.edge_idx],
                            delta_time=torch.tensor(
                                np.array(event.timestamp).astype("float"),
                                requires_grad=True,
                                device=self.device,
                            )
                            - self.memory.get_last_node_update(event.dest),
                        )
                    )
                else:
                    raise Exception(f"Event type not supported {type(event)}")
        return raw_messages

    def _aggregate_messages(
        self,
        processed_messages: Dict[int, List[torch.Tensor]],
    ) -> Dict[int, torch.Tensor]:
        aggregated_messages = {node: None for node in processed_messages}
        for node in processed_messages:
            aggregated_messages[node] = self.message_aggregator(processed_messages[node])
        return aggregated_messages

    def _update_memory(self, messages) -> None:
        for node in messages:
            updated_memory = self.memory_updater((messages[node], self.memory.get_node_memory(node)))

            # here we use flatten to get shape (memory_dim,)
            updated_memory = torch.flatten(updated_memory)
            self.memory.set_node_memory(node, updated_memory)

    def _form_computation_graph(
        self, nodes: np.array, timestamps: np.array
    ) -> Tuple[
        List[List[Tuple[int, int]]],
        List[Dict[Tuple[int, int], int]],
        List[List[int]],
        List[List[int]],
        List[List[Tuple[int, int]]],
    ]:
        """
        This method creates computation graph in form of list of lists of nodes.
        From input nodes and timestamps we sample for each one of them nodes needed to calculate embeddings on certain
        layer.

        If we want to calculate embeddings of input nodes on layer=2, we need to get neighbors of neighbors of input nodes.
        But when we will do "propagation" we will firstly use embeddings of neighbors of neighbors of our input nodes,
        then neighbors of input nodes and this way we get embeddings of input nodes on layer=2.

        Notice one thing, after reversing list,  node_layers[0] will contain nodes that are in node_layers[1] and so on.

        :param nodes: nodes for which we need to calculate embeddings
        :param timestamps: timestamp of appearance of each node


        :return:
            node_layers: List[List[Tuple[int, int]]] - for every layer we return a list containing all nodes at certain
                timestamp for which we will need to calculate embedding. We return all of this info as list of lists.
                At node_layers[0] there are all the nodes at fixed timestamp needed to calculate embeddings.
            mappings: List[Dict[Tuple[int, int], int]] - we build this list of dictionaries  from node_layers. Every
                (node,timestamp) tuple is mapped to index so we can reference it later in embedding calculation
                when building temporal neighborhood concatenated embeddings for graph_attn or for doing summation for
                graph_sum
            global_edge_indexes: List[List[int]] - node_layers[0] contains all nodes at fixed timestamp
                needed to calculate embeddings for given nodes. This way we can use node_layers[0]
                as reference point for indexing.
                At index 0 we have a list of all edge_indexes of edges connecting given node at node_layers[0] to
                temporal neighbors we have at global_neighbors[0]
            global_timestamps: List[List[int]] -  node_layers[0] contains all nodes at fixed timestamp
                needed to calculate embeddings for given nodes. This way we can use node_layers[0]
                as reference point for indexing.
                A t index 0 here we return difference in time between timestamp for node at node_layers[0][0]
                and timestamp of last interaction of their temporal neighbors
            global_neighbors: List[List[Tuple[int, int]]]: again node_layers[0] contains all nodes at fixed timestamp
                needed to calculate embeddings, we can use node_layers[0] as reference point for indexing.
                This is used here, where on index 0 of global_neighbor array are temporal neighbors of (node,timestamp)
                from node_layers[0]
        """
        assert np.issubdtype(timestamps[0], int), f"Expected int type for timestamps, got {type(timestamps[0])}"

        node_layers = [[(n, t) for (n, t) in zip(nodes, timestamps)]]

        sampled_neighborhood: Dict[Tuple[int, int], Tuple[np.array, np.array, np.array]] = {}

        for _ in range(self.num_of_layers):
            prev = node_layers[-1]
            cur_arr = [(n, v) for (n, v) in prev]

            node_arr = []
            for v, t in cur_arr:
                (
                    neighbors,
                    edge_idxs,
                    timestamps,
                ) = (
                    self.temporal_neighborhood.get_neighborhood(v, t, self.num_neighbors)
                    if (v, t) not in sampled_neighborhood
                    else sampled_neighborhood[(v, t)]
                )

                sampled_neighborhood[(v, t)] = (neighbors, edge_idxs, timestamps)

                node_arr.extend([(ni, ti) for (ni, ti) in zip(neighbors, timestamps)])
            cur_arr.extend(node_arr)
            cur_arr = list(set(cur_arr))
            node_layers.append(cur_arr)

        node_layers.reverse()

        # this mapping will be later used to reference node features
        mappings = [{j: i for (i, j) in enumerate(arr)} for arr in node_layers]

        global_edge_indexes = []
        global_timestamps = []
        for v, t in node_layers[0]:
            (
                neighbors,
                edge_idxs,
                timestamps,
            ) = (
                self.temporal_neighborhood.get_neighborhood(v, t, self.num_neighbors)
                if (v, t) not in sampled_neighborhood
                else sampled_neighborhood[(v, t)]
            )

            sampled_neighborhood[(v, t)] = (neighbors, edge_idxs, timestamps)

            global_edge_indexes.append(edge_idxs)
            global_timestamps.append([t - ti for ti in timestamps])  # for neighbors we are always using time diff

        global_neighbors = []
        for v, t in node_layers[0]:
            row = []
            (
                neighbors,
                edge_idxs,
                timestamps,
            ) = sampled_neighborhood[(v, t)]
            for vi, ti in zip(neighbors, timestamps):
                row.append((vi, ti))
            global_neighbors.append(row)

        return (
            node_layers,
            mappings,
            global_edge_indexes,
            global_timestamps,
            global_neighbors,
        )

    def _get_edge_features(self, edge_idx: int) -> torch.Tensor:
        return (
            self.edge_features[edge_idx]
            if edge_idx in self.edge_features
            else torch.rand(self.num_edge_features, requires_grad=True, device=self.device)
        )

    def _get_edges_features(self, edge_idxs: List[int]) -> torch.Tensor:
        edges_features = torch.zeros(self.num_neighbors, self.num_edge_features, device=self.device)
        for i, edge_idx in enumerate(edge_idxs):
            edges_features[i, :] = self._get_edge_features(edge_idx)
        return edges_features

    def _get_graph_data(self, nodes: np.array, timestamps: np.array) -> GraphDataType:
        graph_data_tuple = self._get_graph_sum_data(nodes, timestamps)
        if self.layer_type == TGNLayerType.GraphSumEmbedding:
            return graph_data_tuple
        graph_attn_zeros: torch.Tensor = self.time_encoder(torch.zeros(1, 1, device=self.device)).unsqueeze(0)

        return graph_data_tuple + tuple(graph_attn_zeros)

    def _get_graph_sum_data(self, nodes: np.array, timestamps: np.array) -> GraphSumDataType:
        """
        :param nodes: nodes for which to get data for graph sum embedding layer forward propagation
        :param timestamps: timestamps for given nodes
        :return: GraphSumDataType
        """
        (
            node_layers,
            mappings,
            global_edge_indexes,
            global_timestamps,
            global_neighbors,
        ) = self._form_computation_graph(nodes, timestamps)

        nodes = node_layers[0]

        node_features = torch.zeros(
            len(nodes),
            self.memory_dimension + self.num_node_features,
            device=self.device,
        )

        for i, (node, _) in enumerate(nodes):
            node_feature = (
                self.node_features[node]
                if node in self.node_features
                else torch.zeros(self.num_node_features, requires_grad=True, device=self.device)
            )
            node_memory = torch.tensor(
                self.memory.get_node_memory(node).cpu().detach().numpy(),
                device=self.device,
            )
            node_features[i, :] = torch.concat((node_memory, node_feature))

        edge_features = [self._get_edges_features(node_neighbors) for node_neighbors in global_edge_indexes]

        timestamp_features: List[torch.Tensor] = [
            self.time_encoder(
                torch.tensor(
                    np.array(time, dtype=np.float32),
                    requires_grad=True,
                    device=self.device,
                ).reshape((len(time), 1))
            )
            for time in global_timestamps
        ]

        return (
            node_layers,
            mappings,
            global_edge_indexes,
            global_neighbors,
            node_features,
            edge_features,
            timestamp_features,
        )


def get_message_function_type(
    message_function_type: MessageFunctionType,
) -> MessageFunction:
    if message_function_type == MessageFunctionType.MLP:
        return MessageFunctionMLP
    elif message_function_type == MessageFunctionType.Identity:
        return MessageFunctionIdentity
    else:
        raise Exception(f"Message function type {message_function_type} not yet supported.")


def get_memory_updater_type(memory_updater_type: MemoryUpdaterType) -> MemoryUpdater:
    if memory_updater_type == MemoryUpdaterType.GRU:
        return MemoryUpdaterGRU

    elif memory_updater_type == MemoryUpdaterType.RNN:
        return MemoryUpdaterRNN
    else:
        raise Exception(f"Memory updater type {memory_updater_type} not yet supported.")


def get_message_aggregator_type(
    message_aggregator_type: MessageAggregatorType,
) -> MessageAggregator:
    if message_aggregator_type == MessageAggregatorType.Mean:
        return MeanMessageAggregator

    elif message_aggregator_type == MessageAggregatorType.Last:
        return LastMessageAggregator

    else:
        raise Exception(f"Message aggregator type {message_aggregator_type} not yet supported.")
