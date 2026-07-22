"""
This module represents entry point to temporal graph networks Python implementation of Temporal graph networks for
deep learning on dynamic graphs paper https://arxiv.org/pdf/2006.10637.pdf introduced by E.Rossi [erossi@twitter.com]
during his work in Twitter.

Temporal graph networks(TGNs) is a graph neural network method on dynamic graphs. In the recent years,
graph neural networks have become very popular due to their ability to perform a wide variation of machine learning
tasks on graphs, such as link prediction, node classification and so on. This rise started with Graph convolutional
networks introduced by Kipf et al, following by GraphSAGE introduced by Hamilton et al, and in recent years new
method was presented which introduces attention mechanism to graphs, known as Graph attention networks, by Veličković
et al. Last two methods offer great possibility for inductive learning. But they haven't been specifically developed
to handle different events occurring on graphs, such as node features update, node deletion, edge deletion and so on.

In his work, Rossi et al introduced to us Temporal graph networks which present great possibility to do graph machine
learning on stream of data, use-case occurring more often in recent years.

What we have covered in this module
  * **link prediction** - train your **TGN** to predict new **links/edges** and **node classification** - predict labels of nodes from graph structure and **node/edge** features
  * **graph attention layer** embedding calculation and **graph sum layer** embedding layer calculation
  * **mean** and **last** as message aggregators
  * **mlp** and **identity(concatenation)** as message functions
  * **gru** and **rnn** as memory updater
  * **uniform** temporal neighborhood sampler
  * **memory** store and **raw message store**

as introduced by **[Rossi et al](https://emanuelerossi.co.uk/)**.

Following means **you** can use **TGN** to be able to **predict edges** or  to perform **node classification** tasks, with **graph attention layer** or **graph sum layer**, by using
either **mean** or **last** as message aggregator, **mlp** or **identity** as message function, and finally  **gru** or **rnn** as memory updater.


************************************************** IMPORTANT ******************************************

To start exploring our module, jump to python/mage/tgn/definitions/instances.py and pick one of the implementations,
either TGNEdgesSelfSupervised for self supervised learning on edges or TGNSupervised for supervised learning on nodes classification.

Each of those methods consist of following steps you should look for in the module:
 * self._process_previous_batches - if you follow paper this will include calculating of messages collected for each node
    (python/mage/tgn/definitions/message_function.py), aggregation of messages (python/mage/tgn/definitions/message_aggregator.py)
    and finally memory update part in (python/mage/tgn/definitions/memory_updater.py)
 * self._get_graph_data -> afterwards we create computation graph used by graph attention layer or graph sum layer
 *  self._process_current_batch -> final step includes processing of current batch, updating raw message store from
 interaction events, and preparing our **raw_message_store** for next batches


What is **not** implemented in the module:
  * **node update/deletion events** since they occur very rarely - although we have prepared a terrain
  * **edge deletion** events
  * **time projection** embedding calculation and **identity** embedding calculation since author mentions
    they perform very poorly on all datasets - although it is trivial to add new layer


What we believe we did and author probably failed to do:
 * Embedding calculation seems to be off in authors work on GitHub page: https://github.com/twitter-research/tgn.
    One of our developers of module dropped an issue on GitHub page of twitter-research, but they seem to be too busy,
    can't blame them.
    Problem seems that author doesn't use newly calculated embeddings in next layers, but instead uses raw features from 0th layer,
    which according to paper is wrong.


"""

import dataclasses
import enum
import time
from math import ceil
from typing import Any, Dict, List, Set, Tuple, Union

import mgp
import numpy as np
import torch
import torch.nn as nn
from mage.tgn.constants import MemoryUpdaterType, MessageAggregatorType, MessageFunctionType, TGNLayerType
from mage.tgn.definitions.instances import (
    TGNGraphAttentionEdgeSelfSupervised,
    TGNGraphAttentionSupervised,
    TGNGraphSumEdgeSelfSupervised,
    TGNGraphSumSupervised,
)
from mage.tgn.definitions.tgn import TGN
from mage.tgn.helper.simple_mlp import MLP

###################
# params and classes
##################


# params TGN must receive
class TGNParameters:
    NUM_OF_LAYERS = "num_of_layers"
    LAYER_TYPE = "layer_type"
    MEMORY_DIMENSION = "memory_dimension"
    TIME_DIMENSION = "time_dimension"
    NUM_EDGE_FEATURES = "num_edge_features"
    NUM_NODE_FEATURES = "num_node_features"
    MESSAGE_DIMENSION = "message_dimension"
    NUM_NEIGHBORS = "num_neighbors"
    EDGE_FUNCTION_TYPE = "edge_message_function_type"
    MESSAGE_AGGREGATOR_TYPE = "message_aggregator_type"
    MEMORY_UPDATER_TYPE = "memory_updater_type"
    NUM_ATTENTION_HEADS = "num_attention_heads"
    DEVICE = "device"


class OptimizerParameters:
    LEARNING_RATE = "learning_rate"
    WEIGHT_DECAY = "weight_decay"


class MemgraphObjectsProperties:
    NODE_FEATURES_PROPERTY = "node_features_property"
    EDGE_FEATURES_PROPERTY = "edge_features_property"
    NODE_LABELS_PROPERTY = "node_label_property"


class OtherProperties:
    LEARNING_TYPE = "learning_type"
    DEVICE_TYPE = "device_type"
    BATCH_SIZE = "batch_size"


class LearningType(enum.Enum):
    Supervised = "supervised"
    SelfSupervised = "self_supervised"


class DeviceType(enum.Enum):
    CUDA = "cuda"
    CPU = "cpu"


class TGNMode(enum.Enum):
    Train = "train"
    Eval = "eval"


@dataclasses.dataclass
class QueryModuleTGN:
    config: Dict[str, Any]
    tgn: TGN
    criterion: nn.BCELoss
    optimizer: torch.optim.Adam
    device: torch.device
    m_loss: List[float]  # mean loss
    mlp: MLP
    tgn_mode: TGNMode
    learning_type: LearningType
    # used in negative sampling for self_supervised
    all_edges: Set[Tuple[int, int]]
    # to get all embeddings
    all_embeddings: Dict[int, np.array]
    results_per_epochs: Dict[int, List[mgp.Record]]
    current_epoch: int
    global_edge_count: int
    train_eval_index_split: int
    memgraph_objects_properties: Dict[str, Any]


@dataclasses.dataclass
class QueryModuleTGNBatch:
    current_batch_size: int
    sources: np.array
    destinations: np.array
    timestamps: np.array
    edge_idxs: np.array
    node_features: Dict[int, torch.Tensor]
    edge_features: Dict[int, torch.Tensor]
    batch_size: int
    labels: np.array


##############################
# global tgn training variables
##############################


query_module_tgn: QueryModuleTGN
query_module_tgn_batch: QueryModuleTGNBatch

##############################
# constants
##############################

EPOCH_START = 1

DEFINED_INPUT_TYPES = {
    "learning_type": str,
    "batch_size": int,
    TGNParameters.NUM_OF_LAYERS: int,
    TGNParameters.LAYER_TYPE: str,
    TGNParameters.MEMORY_DIMENSION: int,
    TGNParameters.TIME_DIMENSION: int,
    TGNParameters.NUM_EDGE_FEATURES: int,
    TGNParameters.NUM_NODE_FEATURES: int,
    TGNParameters.MESSAGE_DIMENSION: int,
    TGNParameters.NUM_NEIGHBORS: int,
    TGNParameters.EDGE_FUNCTION_TYPE: str,
    TGNParameters.MESSAGE_AGGREGATOR_TYPE: str,
    TGNParameters.MEMORY_UPDATER_TYPE: str,
}

DEFAULT_VALUES = {
    OtherProperties.LEARNING_TYPE: "self_supervised",
    OtherProperties.BATCH_SIZE: 200,
    TGNParameters.NUM_OF_LAYERS: 2,
    TGNParameters.LAYER_TYPE: "graph_attn",
    TGNParameters.MEMORY_DIMENSION: 100,
    TGNParameters.TIME_DIMENSION: 100,
    TGNParameters.NUM_EDGE_FEATURES: 50,
    TGNParameters.NUM_NODE_FEATURES: 50,
    TGNParameters.MESSAGE_DIMENSION: 100,
    TGNParameters.NUM_NEIGHBORS: 15,
    TGNParameters.EDGE_FUNCTION_TYPE: "identity",
    TGNParameters.MESSAGE_AGGREGATOR_TYPE: "last",
    TGNParameters.MEMORY_UPDATER_TYPE: "gru",
    TGNParameters.NUM_ATTENTION_HEADS: 1,
    MemgraphObjectsProperties.NODE_FEATURES_PROPERTY: "features",
    MemgraphObjectsProperties.EDGE_FEATURES_PROPERTY: "features",
    MemgraphObjectsProperties.NODE_LABELS_PROPERTY: "label",
    OptimizerParameters.LEARNING_RATE: 1e-4,
    OptimizerParameters.WEIGHT_DECAY: 5e-5,
    OtherProperties.DEVICE_TYPE: "cuda",
}


#############################
# global helpers
#############################
def update_epoch_counter() -> int:
    global query_module_tgn

    query_module_tgn.current_epoch += 1
    return query_module_tgn.current_epoch


def get_current_epoch() -> int:
    global query_module_tgn
    return query_module_tgn.current_epoch


def get_current_batch() -> int:
    global query_module_tgn
    return len(query_module_tgn.results_per_epochs[query_module_tgn.current_epoch])


def initialize_results_per_epoch(current_epoch: int) -> None:
    global query_module_tgn
    query_module_tgn.results_per_epochs[current_epoch] = []


def set_global_edge_count(new_global_edge_count: int) -> int:
    global query_module_tgn
    query_module_tgn.global_edge_count = new_global_edge_count
    return query_module_tgn.global_edge_count


def set_current_batch_size(new_current_batch_size: int) -> int:
    global query_module_tgn_batch
    query_module_tgn_batch.current_batch_size = new_current_batch_size
    return query_module_tgn_batch.current_batch_size


def append_batch_record_curr_epoch(current_epoch: int, record: mgp.Record) -> Dict[int, List[mgp.Record]]:
    global query_module_tgn
    assert current_epoch in query_module_tgn.results_per_epochs, f"Current epoch not defined"
    query_module_tgn.results_per_epochs[current_epoch].append(record)
    return query_module_tgn.results_per_epochs


def get_output_records() -> List[mgp.Record]:
    global query_module_tgn, EPOCH_START
    output_records = []

    for i in range(EPOCH_START, len(query_module_tgn.results_per_epochs) + 1):
        output_records.extend(query_module_tgn.results_per_epochs[i])
    return output_records


def is_tgn_initialized() -> bool:
    global query_module_tgn
    if query_module_tgn.tgn is None:
        return False
    return True


def get_link_score(src_tensor: torch.Tensor, dest_tensor: torch.Tensor) -> torch.Tensor:
    global query_module_tgn
    # along columns
    x = torch.cat([src_tensor, dest_tensor], dim=1)
    return query_module_tgn.mlp(x).squeeze(dim=0)


#####################################

# init function

#####################################


def set_tgn(
    learning_type: LearningType,
    device_type: DeviceType,
    tgn_config: Dict[str, Any],
    optimizer_config: Dict[str, Any],
    memgraph_objects_properties_config: Dict[str, Any],
) -> None:
    global query_module_tgn, EPOCH_START

    if device_type == device_type.CUDA and torch.cuda.is_available():
        device = torch.device("cuda")
    else:
        device = torch.device("cpu")

    tgn_config[TGNParameters.DEVICE] = device

    if learning_type == LearningType.SelfSupervised:
        tgn, mlp = get_tgn_self_supervised(tgn_config, device)
    else:
        tgn, mlp = get_tgn_supervised(tgn_config, device)

    criterion = torch.nn.BCELoss()
    optimizer = torch.optim.Adam(
        tgn.parameters(),
        lr=optimizer_config[OptimizerParameters.LEARNING_RATE],
        weight_decay=optimizer_config[OptimizerParameters.WEIGHT_DECAY],
    )

    query_module_tgn = QueryModuleTGN(
        config=tgn_config,
        tgn=tgn,
        criterion=criterion,
        optimizer=optimizer,
        device=device,
        m_loss=[],
        mlp=mlp,
        tgn_mode=TGNMode.Train,  # we start in train mode
        learning_type=learning_type,
        all_edges=set(),
        all_embeddings={},
        results_per_epochs={},
        current_epoch=EPOCH_START,
        global_edge_count=0,
        train_eval_index_split=0,  # this number represent number of edges when set_eval function was called
        memgraph_objects_properties=memgraph_objects_properties_config,
    )


def get_tgn_self_supervised(config: Dict[str, Any], device: torch.device) -> Tuple[TGN, MLP]:
    """
    Set parameters for self supervised learning. Here we try to predict edges.
    """

    if config[TGNParameters.LAYER_TYPE] == TGNLayerType.GraphSumEmbedding:
        tgn = TGNGraphSumEdgeSelfSupervised(**config).to(device)
    else:
        tgn = TGNGraphAttentionEdgeSelfSupervised(**config).to(device)

    # When TGN outputs embeddings for source nodes and destination nodes,
    # since we are working with edges and edge predictions we will concatenate their features together
    # and get prediction with MLP whether it is edge or it isn't
    # Other possibility would be Hadamard product
    # ( https://en.wikipedia.org/wiki/Hadamard_product_(matrices) it is a fancy name for dot product)
    # of source embeddings and destination embeddings, but we went with concat since author used concatenation
    # and not Hadamard product, in original implementation
    mlp_in_features_dim = (config[TGNParameters.MEMORY_DIMENSION] + config[TGNParameters.NUM_NODE_FEATURES]) * 2

    mlp = MLP([mlp_in_features_dim, mlp_in_features_dim // 2, 1]).to(device=device)

    return tgn, mlp


def get_tgn_supervised(config: Dict[str, Any], device: torch.device) -> Tuple[TGN, MLP]:
    """ """

    if config[TGNParameters.LAYER_TYPE] == TGNLayerType.GraphSumEmbedding:
        tgn = TGNGraphSumSupervised(**config).to(device)
    else:
        tgn = TGNGraphAttentionSupervised(**config).to(device)

    mlp_in_features_dim = config[TGNParameters.MEMORY_DIMENSION] + config[TGNParameters.NUM_NODE_FEATURES]

    # used as probability calculator for label
    mlp = MLP([mlp_in_features_dim, 64, 1]).to(device=device)

    return tgn, mlp


#
# Helper functions
#


def sample_negative(negative_num: int) -> Tuple[np.array, np.array]:
    """
    Currently sampling of negative nodes is done in completely random fashion, and it is possible to sample
    source-dest pair that are real edges
    """
    global query_module_tgn
    all_edges = query_module_tgn.all_edges
    all_src = list(set([src for src, dest in all_edges]))
    all_dest = list(set([src for src, dest in all_edges]))

    return np.random.choice(all_src, negative_num, replace=True), np.random.choice(all_dest, negative_num, replace=True)


def unpack_tgn_batch_data():
    global query_module_tgn_batch
    return dataclasses.astuple(query_module_tgn_batch)


def update_mode_reset_grads_check_dims() -> None:
    global query_module_tgn, query_module_tgn_batch

    if query_module_tgn.tgn_mode == TGNMode.Train:
        # set training mode
        query_module_tgn.tgn.train()

        query_module_tgn.optimizer.zero_grad()
        query_module_tgn.tgn.detach_tensor_grads()

        # todo add so that we only work with latest 128 neighbors
        # query_module_tgn.tgn.subsample_neighborhood()
    else:
        query_module_tgn.tgn.eval()

    (
        _,
        sources,
        destinations,
        timestamps,
        edge_idxs,
        _,
        edge_features,
        _,
        labels,
    ) = unpack_tgn_batch_data()
    assert (
        len(sources) == len(destinations) == len(timestamps) == len(edge_features) == len(edge_idxs) == len(labels)
    ), f"Batch size training error"


def update_embeddings(
    embeddings_source: np.array,
    embeddings_dest: np.array,
    sources: np.array,
    destinations: np.array,
) -> None:
    global query_module_tgn
    for i, node in enumerate(sources):
        query_module_tgn.all_embeddings[node] = embeddings_source[i]

    for i, node in enumerate(destinations):
        query_module_tgn.all_embeddings[node] = embeddings_dest[i]


#
# Batch processing - self_supervised
#
def process_batch_self_supervised() -> float:
    """
    Uses sources, destinations, timestamps, edge_features and node_features from transactions.
    It is possible that current_batch_size is not always consistent, but it is always greater than minimum required.
    """
    global query_module_tgn, query_module_tgn_batch

    # do all necessary checks and updates of gradients
    update_mode_reset_grads_check_dims()

    (
        _,
        sources,
        destinations,
        timestamps,
        edge_idxs,
        node_features,
        edge_features,
        _,
        _,
    ) = unpack_tgn_batch_data()

    current_batch_size = len(sources)
    negative_src, negative_dest = sample_negative(current_batch_size)

    graph_data = (
        sources,
        destinations,
        negative_src,
        negative_dest,
        timestamps,
        edge_idxs,
        edge_features,
        node_features,
    )

    embeddings, embeddings_negative = query_module_tgn.tgn(graph_data)

    embeddings_source = embeddings[:current_batch_size]
    embeddings_dest = embeddings[current_batch_size:]

    embeddings_source_neg = embeddings_negative[:current_batch_size]
    embeddings_dest_neg = embeddings_negative[current_batch_size:]

    # first row concatenation
    src_embeddings, dest_embeddings = torch.cat([embeddings_source, embeddings_source_neg], dim=0), torch.cat(
        [embeddings_dest, embeddings_dest_neg], dim=0
    )
    # score shape = (num_positive_edges + num_negative_edges, 1) ->
    # num_positive_edges == num_negative_edges == current_batch_size
    score = get_link_score(src_embeddings, dest_embeddings)

    pos_score = score[:current_batch_size]
    neg_score = score[current_batch_size:]
    pos_prob, neg_prob = pos_score.sigmoid(), neg_score.sigmoid()

    if query_module_tgn.tgn_mode == TGNMode.Train:
        pos_label = torch.ones(current_batch_size, dtype=torch.float, device=query_module_tgn.device)
        neg_label = torch.zeros(current_batch_size, dtype=torch.float, device=query_module_tgn.device)
        # use reshape to get 1 dimension in every case
        loss = query_module_tgn.criterion(pos_prob.reshape((-1,)), pos_label) + query_module_tgn.criterion(
            neg_prob.reshape((-1,)), neg_label
        )

        loss.backward()
        query_module_tgn.optimizer.step()
        query_module_tgn.m_loss.append(loss.item())
    pos_prob_cpu = pos_prob.reshape((-1,)).detach().cpu()
    neg_prob_cpu = neg_prob.reshape((-1,)).detach().cpu()
    # todo antoniofilipovic - update once we have logging API
    print(
        "POS PROB | NEG PROB",
        pos_prob_cpu,
        neg_prob_cpu,
    )
    pred_score = np.concatenate(
        [
            pos_prob_cpu.numpy(),
            neg_prob_cpu.numpy(),
        ]
    )
    true_label = np.concatenate([np.ones(current_batch_size), np.zeros(current_batch_size)])
    # rint - round int to nearest even number
    # x = 0, x <= 0.5
    # x = 1, x > 0.5
    # sum correct labels, divide by total number of labels
    precision = np.sum(np.rint(true_label) == np.rint(pred_score)) * 1.0 / len(true_label)

    # update embeddings to the newest ones that we can return on user request
    update_embeddings(
        embeddings_source.detach().cpu().numpy(),
        embeddings_dest.detach().cpu().numpy(),
        sources,
        destinations,
    )

    return precision


#
# Training - supervised
#


def process_batch_supervised() -> float:
    global query_module_tgn, query_module_tgn_batch

    # do all necessary checks and updates of gradients
    update_mode_reset_grads_check_dims()

    (
        _,
        sources,
        destinations,
        timestamps,
        edge_idxs,
        node_features,
        edge_features,
        _,
        labels,
    ) = unpack_tgn_batch_data()

    current_batch_size = len(sources)

    graph_data = (
        sources,
        destinations,
        timestamps,
        edge_idxs,
        edge_features,
        node_features,
    )

    embeddings = query_module_tgn.tgn(graph_data)

    embeddings_source = embeddings[:current_batch_size]
    embeddings_dest = embeddings[current_batch_size:]

    x = torch.cat([embeddings_source, embeddings_dest], dim=0)  # along rows

    score = query_module_tgn.mlp(x).squeeze(dim=0)

    src_score = score[:current_batch_size]
    dest_score = score[current_batch_size:]

    src_prob, dest_prob = src_score.sigmoid(), dest_score.sigmoid()

    pred_score = np.concatenate(
        [
            (src_prob.squeeze()).detach().cpu().numpy(),
            (dest_prob.squeeze()).detach().cpu().numpy(),
        ]
    )
    true_label = np.concatenate([np.array(labels[:, 0]), np.array(labels[:, 1])])

    precision = np.sum(np.rint(true_label) == np.rint(pred_score)) * 1.0 / len(true_label)

    # update embeddings to newest ones that we can return on user request
    update_embeddings(
        embeddings_source.detach().cpu().numpy(),
        embeddings_dest.cpu().detach().cpu().numpy(),
        sources,
        destinations,
    )

    if query_module_tgn.tgn_mode == TGNMode.Eval:
        return precision

    # backprop only in case of training
    with torch.no_grad():
        src_label = torch.tensor(labels[:, 0], dtype=torch.float, device=query_module_tgn.device)
        dest_label = torch.tensor(labels[:, 1], dtype=torch.float, device=query_module_tgn.device)

    loss = query_module_tgn.criterion(src_prob.squeeze(), src_label.squeeze()) + query_module_tgn.criterion(
        dest_prob.squeeze(), dest_label.squeeze()
    )
    loss.backward()
    query_module_tgn.optimizer.step()
    query_module_tgn.m_loss.append(loss.item())

    return precision


def create_torch_tensor(feature: Union[None, Tuple], num_features: int) -> torch.Tensor:
    if feature is None:
        np_feature = np.random.uniform(0, 1, num_features)  # uniformly sample features from 0 to 1
    else:
        np_feature = np.array(feature)
    return torch.tensor(
        np_feature,
        requires_grad=True,
        device=query_module_tgn.device,
        dtype=torch.float,
    )


def parse_mgp_edges_into_tgn_batch(edges: mgp.List[mgp.Edge]) -> QueryModuleTGNBatch:
    global query_module_tgn_batch, query_module_tgn

    for edge in edges:
        source = edge.from_vertex
        src_id = edge.from_vertex.id

        dest = edge.to_vertex
        dest_id = int(edge.to_vertex.id)

        # maybe this is not best practice, but since we are calling this
        # function only for processing of edges, we can also
        # update all edges to for negative sampling later used later on
        query_module_tgn.all_edges.add((src_id, dest_id))

        node_features_property = query_module_tgn.memgraph_objects_properties[
            MemgraphObjectsProperties.NODE_FEATURES_PROPERTY
        ]
        edge_features_property = query_module_tgn.memgraph_objects_properties[
            MemgraphObjectsProperties.EDGE_FEATURES_PROPERTY
        ]
        node_label_property = query_module_tgn.memgraph_objects_properties[
            MemgraphObjectsProperties.NODE_LABELS_PROPERTY
        ]
        src_features = source.properties.get(node_features_property, None)
        dest_features = dest.properties.get(node_features_property, None)

        src_label = source.properties.get(node_label_property, 0)
        dest_label = dest.properties.get(node_label_property, 0)

        timestamp = edge.id
        edge_idx = int(edge.id)

        edge_feature = edge.properties.get(edge_features_property, None)

        query_module_tgn_batch.node_features[src_id] = create_torch_tensor(
            src_features, query_module_tgn.config[TGNParameters.NUM_NODE_FEATURES]
        )
        query_module_tgn_batch.node_features[dest_id] = create_torch_tensor(
            dest_features, query_module_tgn.config[TGNParameters.NUM_NODE_FEATURES]
        )
        query_module_tgn_batch.edge_features[edge_idx] = create_torch_tensor(
            edge_feature, query_module_tgn.config[TGNParameters.NUM_EDGE_FEATURES]
        )

        query_module_tgn_batch.sources = np.append(query_module_tgn_batch.sources, src_id)
        query_module_tgn_batch.destinations = np.append(query_module_tgn_batch.destinations, dest_id)
        query_module_tgn_batch.timestamps = np.append(query_module_tgn_batch.timestamps, timestamp)
        query_module_tgn_batch.edge_idxs = np.append(query_module_tgn_batch.edge_idxs, edge_idx)
        query_module_tgn_batch.labels = np.append(
            query_module_tgn_batch.labels, np.array([[src_label, dest_label]]), axis=0
        )
    return query_module_tgn_batch


def reset_tgn_batch(batch_size: int) -> None:
    global query_module_tgn_batch
    query_module_tgn_batch = QueryModuleTGNBatch(
        0,
        np.empty((0, 1), dtype=int),
        np.empty((0, 1), dtype=int),
        np.empty((0, 1), dtype=int),
        np.empty((0, 1), dtype=int),
        {},
        {},
        batch_size,
        np.empty((0, 2), dtype=int),
    )


def reset_tgn() -> None:
    global query_module_tgn

    # reset whole tgn
    query_module_tgn.all_embeddings = {}
    reset_tgn_batch(0)
    query_module_tgn.all_edges = set()


def process_epoch_batch() -> mgp.Record:
    batch_start_time = time.time()
    precision = (
        process_batch_self_supervised()
        if query_module_tgn.learning_type == LearningType.SelfSupervised
        else process_batch_supervised()
    )
    batch_process_time = time.time() - batch_start_time

    record = mgp.Record(
        epoch_num=get_current_epoch(),
        batch_num=get_current_batch() + 1,  # this is a new record batch
        batch_process_time=round(batch_process_time, 2),
        precision=round(precision, 2),
        batch_type=query_module_tgn.tgn_mode.name,
    )

    # add same logging as in core
    print(
        f"EPOCH {get_current_epoch()} || BATCH {get_current_batch()}, | batch_process_time={batch_process_time}  | precision={precision}"
    )

    return record


def train_eval_epochs(num_epochs: int, train_edges: List[mgp.Edge], eval_edges: List[mgp.Edge]) -> None:
    global query_module_tgn, query_module_tgn_batch

    batch_size = query_module_tgn_batch.batch_size
    num_train_edges = len(train_edges)
    num_train_batches = ceil(num_train_edges / batch_size)

    num_eval_edges = len(eval_edges)
    num_eval_batches = ceil(num_eval_edges / batch_size)
    assert batch_size > 0

    for epoch in range(num_epochs):
        # update global epoch counter
        update_epoch_counter()

        # initialize container for current epochs where we save records for
        # precision results and batch processing time
        initialize_results_per_epoch(get_current_epoch())

        # on every epoch training tgn should have clean state,
        # although we update parameters, memory should be empty,
        # temporal neighborhood should be empty and message store should be empty
        # because if it isn't we have problem with information leakage from future
        # to current training samples
        query_module_tgn.tgn.init_memory()
        query_module_tgn.tgn.init_temporal_neighborhood()
        query_module_tgn.tgn.init_message_store()

        query_module_tgn.all_edges = set()
        query_module_tgn.m_loss = []

        reset_tgn_batch(batch_size=batch_size)

        # when we update here tgn_mode, later when we call process_batch_self_supervised
        # it will change tgn mode to .train() so no worries
        query_module_tgn.tgn_mode = TGNMode.Train

        for i in range(num_train_batches):
            # sample edges we need
            start_index_train_batch = i * batch_size
            end_index_train_batch = min((i + 1) * batch_size, num_train_edges)
            current_edges_batch = train_edges[start_index_train_batch:end_index_train_batch]

            # prepare batch
            parse_mgp_edges_into_tgn_batch(current_edges_batch)
            batch_result_record = process_epoch_batch()
            append_batch_record_curr_epoch(get_current_epoch(), batch_result_record)

            # reset for next batch
            reset_tgn_batch(batch_size=batch_size)

        # here we need to change mode to eval
        # also later when we call process_batch_self_supervised
        # it will change tgn mode to .eval()
        query_module_tgn.tgn_mode = TGNMode.Eval
        for i in range(num_eval_batches):
            # sample edges we need
            start_index_eval_batch = i * batch_size
            end_index_eval_batch = min((i + 1) * batch_size, num_eval_edges)
            current_edges_batch = eval_edges[start_index_eval_batch:end_index_eval_batch]
            # prepare batch
            parse_mgp_edges_into_tgn_batch(current_edges_batch)
            batch_result_record = process_epoch_batch()
            append_batch_record_curr_epoch(get_current_epoch(), batch_result_record)

            # reset for next batch
            reset_tgn_batch(batch_size=batch_size)


#####################################################

# all available read_procs


#####################################################
@mgp.read_proc
def predict_link_score(ctx: mgp.ProcCtx, src: mgp.Vertex, dest: mgp.Vertex) -> mgp.Record(prediction=mgp.Number):
    """
    If you were doing link prediction, with this function you can input some of your vertices, and get the predictions
    Be careful to input vertices in correct order (src->dest) otherwise you might get wrong prediction

    :params src: src vertex in prediction
    :params dest: dest vertex in prediction

    :return prediction: score between 0 and 1
    """
    global query_module_tgn

    embedding_source: List[float] = query_module_tgn.all_embeddings[int(src.id)]
    embedding_dest = query_module_tgn.all_embeddings[int(dest.id)]

    embedding_src_torch = torch.tensor(embedding_source, device=query_module_tgn.device, dtype=torch.float).reshape(
        1, -1
    )
    embedding_dest_torch = torch.tensor(embedding_dest, device=query_module_tgn.device, dtype=torch.float).reshape(
        1, -1
    )

    # column concatenation
    score = get_link_score(embedding_src_torch, embedding_dest_torch)
    return mgp.Record(prediction=float(score))


@mgp.read_proc
def train_and_eval(
    ctx: mgp.ProcCtx, num_epochs: int
) -> mgp.Record(
    epoch_num=mgp.Number,
    batch_num=mgp.Number,
    batch_process_time=mgp.Number,
    precision=mgp.Number,
    batch_type=str,
):
    """
    After calling this function from ctx we will get all edges currently in database, split them in ratio of
    train and eval edges if function set_mode("eval") was called at some point and use training edges to train
    further TGN and eval edges to evaluate our model.

    If you didn't call function set_mode("eval"), you won't be able to do train and eval.

    :param num_epochs: number of epochs used for training and evaluation
    :train_eval_percent_split: dataset split ratio on train and eval


    :return: mgp.Record(): empty record if everything was fine
    """

    global query_module_tgn

    if not is_tgn_initialized():
        raise Exception("TGN is not initialized still. Call `set_params` function in order to initialize it.")

    vertices = ctx.graph.vertices
    curr_all_edges = []
    for vertex in vertices:
        curr_all_edges.extend(list(vertex.out_edges))

    curr_all_edges = sorted(curr_all_edges, key=lambda x: x.id)

    # note: if you didn't call mode switch to eval, you can't
    # still do epoch training
    if query_module_tgn.train_eval_index_split == 0:
        raise Exception("Can't call train and eval if you didn't change TGN mode to 'eval'")

    train_eval_epochs(
        num_epochs=num_epochs,
        train_edges=curr_all_edges[: query_module_tgn.train_eval_index_split],
        eval_edges=curr_all_edges[query_module_tgn.train_eval_index_split :],
    )
    # get all records for every epoch and every batch inside it as results
    return get_output_records()


@mgp.read_proc
def get_results(
    ctx: mgp.ProcCtx,
) -> mgp.Record(
    epoch_num=mgp.Number,
    batch_num=mgp.Number,
    batch_process_time=mgp.Number,
    precision=mgp.Number,
    batch_type=str,
):
    """
    This method returns all results from training and evaluation on all epochs

    :return: mgp.Record(): List of records of training and evaluation stats

    """
    # get all records for every epoch and every batch inside it as results
    if not is_tgn_initialized():
        raise Exception("TGN is not initialized still. Call `set_params` function in order to initialize it.")

    return get_output_records()


@mgp.read_proc
def set_eval(ctx: mgp.ProcCtx) -> mgp.Record(message=str):
    """
    Purpose of this function is to switch mode from "train" to "eval" at some point during your stream.
    At that point, we will save current edge count, and this information will later be used in function
    `train_and_eval` to split edges from Memgraph in train and eval set

    :return: mgp.Record(): empty record if everything was fine
    """
    global query_module_tgn

    if not is_tgn_initialized():
        raise Exception("TGN is not initialized still. Call `set_params` function in order to initialize it.")

    query_module_tgn.train_eval_index_split = query_module_tgn.global_edge_count
    query_module_tgn.tgn_mode = TGNMode.Eval

    return mgp.Record(message=f"TGN mode changed to 'eval'.")


@mgp.read_proc
def revert_from_database(ctx: mgp.ProcCtx) -> mgp.Record():
    """
    todo implement
    Revert from database and potential file in var/log/ to which we can save params
    """
    raise NotImplementedError("You can check what is implemented at our docs page: https://memgraph.com/docs/mage")


@mgp.read_proc
def save_tgn_params(ctx: mgp.ProcCtx) -> mgp.Record():
    """
    todo implement
    After every batch we could add saving params as checkpoints to var/log/memgraph
    This is how it is done usually in ML
    """
    raise NotImplementedError("You can check what is implemented at our docs page: https://memgraph.com/docs/mage")


@mgp.read_proc
def reset(ctx: mgp.ProcCtx) -> mgp.Record(message=str):
    reset_tgn()
    return mgp.Record(message="Reset was successful.")


@mgp.read_proc
def get(ctx: mgp.ProcCtx) -> mgp.Record(node=mgp.Vertex, embedding=mgp.List[float]):
    """
    Get all embeddings of nodes created by TGN. These are final embeddings, after num_layers of processing

    One note here: embeddings are not for same timestamp, since one node could have last interaction at time
    t1 and current timestamp can be tn, where t1<tn

    We can't update embedding of node if it doesn't have any interactions, but we can update node's memory, so next
    time it appears in some interaction event, it won't suffer from memory staleness problem as mentioned in original
    paper

    :param edges: list of edges to preprocess, and if current batch size is big enough use for training or evaluation

    :return: mgp.Record(): empty record if everything was fine
    """
    global query_module_tgn

    if not is_tgn_initialized():
        raise Exception("TGN is not initialized still. Call `set_params` function in order to initialize it.")

    embeddings_dict = {}

    for node_id, embedding in query_module_tgn.all_embeddings.items():
        embeddings_dict[node_id] = [float(e) for e in embedding]

    return [
        mgp.Record(node=ctx.graph.get_vertex_by_id(node_id), embedding=embedding)
        for node_id, embedding in embeddings_dict.items()
    ]


@mgp.read_proc
def update(ctx: mgp.ProcCtx, edges: mgp.List[mgp.Edge]) -> mgp.Record():
    """
    Purpose of following function is to process edges which are created in Memgraph, and get features from
    nodes or edges if they are present and save all that data so when batch is greater than
    predefined size `batch_size` saved in object `query_mode_tgn` "set_params",
    this function will call training on those edges of TGN module. This represents one batch
    and until you call tgn.set_mode("eval") all those edges will be used as training edges.

    If you have a stream of data and you expect let's say 20000 edges, you can "split" your stream of data
    in "train" set and "eval" set by calling method tgn.set_mode("eval").


    Once again you should not call this function yourself but set trigger
    on --> CREATE event (edge create event), and Memgraph will give edges as input

    Your toy example could look like following

    CREATE INDEX ON :User(id);
    CREATE INDEX ON :Item(id);
    CALL tgn.set_params("self_supervised", 200, 2, "graph_attn", 100, 100, 20, 20, 100, 10, "identity", "last", "gru", 1) YIELD *;
    MERGE (a:User {id: 'A1BHUGKLYW6H7V', profile_name:'P. Lecuyer'}) MERGE (b:Item {id: 'B0007MCVQ2'}) MERGE (a)-[:REVIEWED { features: [161.0,..., 0.9238], ...}]->(b);
    Here create more edges
    CALL tgn.set_mode("eval") YIELD *;
    Here create some edges used for evaluation
    CALL tgn.train_and_eval(5) YIELD * RETURN *;

    This way all edges **until** you call `CALL tgn.set_mode("eval") YIELD *;` will be used for **training**,
    and all edges after such call will be used for **evaluation**.

    After you make a query `CALL tgn.train_and_eval(5) YIELD * RETURN *;`, we will get all edges from our database,
    and because you called tgn.set_mode("eval") at some point, we save at which point it happened at we will split
    train and eval edges in same manner.

    :param edges: list of edges to preprocess, and if current batch size is big enough use for training or evaluation

    :return: mgp.Record(): empty record if everything was fine
    """
    global query_module_tgn_batch, query_module_tgn

    if not is_tgn_initialized():
        raise Exception("TGN is not initialized still. Call `set_params` function in order to initialize it.")

    num_edges = len(edges)

    # we track number of edges so
    set_global_edge_count(query_module_tgn.global_edge_count + num_edges)

    # update current batch size with new edges
    set_current_batch_size(query_module_tgn_batch.current_batch_size + num_edges)

    # we update our batch with current edges
    parse_mgp_edges_into_tgn_batch(edges)
    # if batch is still not full, we don't go to "train" or "eval" of TGN
    if query_module_tgn_batch.current_batch_size < query_module_tgn_batch.batch_size:
        return mgp.Record()
    # this is just check if we have initialized list to save records of batches for training or evaluation
    if get_current_epoch() not in query_module_tgn.results_per_epochs:
        initialize_results_per_epoch(get_current_epoch())

    # process epoch in self_supervised or supervised mode in a given mode which
    # can be "train" or "eval"
    batch_result_record = process_epoch_batch()

    append_batch_record_curr_epoch(get_current_epoch(), batch_result_record)

    # reset for next batch
    reset_tgn_batch(batch_size=query_module_tgn_batch.batch_size)

    return mgp.Record()


@mgp.read_proc
def set_params(
    ctx: mgp.ProcCtx,
    params: mgp.Map,
) -> mgp.Record():
    """
    With following function you can define parameters used in TGN, as well as what kind of learning you want
    to do with TGN module.

    If you set TGN to "self_supervised" mode, it will try to predict new edges, and with "supervised" mode it will try
    to predict labels
    How to call this method:
        CALL tgn.set_params({learning_type:'self_supervised', batch_size:200, num_of_layers:2, layer_type:'graph_attn', memory_dimension:20, time_dimension:50, num_edge_features:20,
        num_node_features:20, message_dimension:100, num_neighbors:15, edge_message_function_type:'identity',message_aggregator_type:'last', memory_updater_type:'gru', num_attention_heads:1});
    Warning: Every time you call this function, old TGN object is cleared and process of learning params is
    restarted

    :param params: Dict containing following keys:
        learning_type: "self_supervised" or "supervised" depending on if you want to predict edges or node labels
        batch_size: size of batch to process by TGN, recommended size 200
        num_of_layers: number of layers of graph neural network, 2 is optimal size, GNNs perform worse with bigger size
        layer_type: "graph_attn" or "graph_sum" layer type as defined in original paper
        memory_dimension: dimension of memory tensor of each node
        time_dimension: dimension of time vector from "time2vec" paper
        num_edge_features: number of edge features we will use from each edge
        num_node_features: number of expected node features
        message_dimension: dimension of message, only used if you use MLP as message function type, otherwise ignored
        num_neighbors: number of sampled neighbors
        edge_message_function_type: message function type, "identity" for concatenation or "mlp" for projection
        message_aggregator_type: message aggregator type, "mean" or "last"
        memory_updater_type: memory updater type, "gru" or "rnn"
        [optional] num_attention_heads: number of attention heads used if **only** if you define "graph_attn" as layer type
        [optional] learning_rate: learning rate for optimizer
        [optional] weight_decay: weight decay used in optimizer
        [optional] device_type: type of device you want to use for training - cuda or cpu
        [optional] node_features_property: name of features property on nodes from which we read features
        [optional] edge_features_property: name of features property on edges from which we read features
        [optional] node_label_property: name of label property on nodes from which we read features

    :return: mgp.Record(): empty record if everything was fine
    """
    global query_module_tgn_batch, DEFINED_INPUT_TYPES, DEFAULT_VALUES

    # function checks if input values in dictionary are correctly typed
    def is_correctly_typed(defined_types, input_values):
        if isinstance(defined_types, dict) and isinstance(input_values, dict):
            # defined_types is a dict of types
            return all(
                k in input_values  # check if exists
                and is_correctly_typed(defined_types[k], input_values[k])  # check for correct type
                for k in defined_types
            )
        elif isinstance(defined_types, type):
            return isinstance(input_values, defined_types)
        else:
            return False

    params = {**DEFAULT_VALUES, **params}  # override any default parameters
    print(params)
    if not is_correctly_typed(DEFINED_INPUT_TYPES, params):
        raise Exception(f"Input dictionary is not correctly typed. Expected following types {DEFINED_INPUT_TYPES}.")

    learning_type: str = params[OtherProperties.LEARNING_TYPE]
    batch_size: int = params[OtherProperties.BATCH_SIZE]

    reset_tgn_batch(batch_size)

    tgn_config = {
        TGNParameters.NUM_OF_LAYERS: params[TGNParameters.NUM_OF_LAYERS],
        TGNParameters.MEMORY_DIMENSION: params[TGNParameters.MEMORY_DIMENSION],
        TGNParameters.TIME_DIMENSION: params[TGNParameters.TIME_DIMENSION],
        TGNParameters.NUM_EDGE_FEATURES: params[TGNParameters.NUM_EDGE_FEATURES],
        TGNParameters.NUM_NODE_FEATURES: params[TGNParameters.NUM_NODE_FEATURES],
        TGNParameters.MESSAGE_DIMENSION: params[TGNParameters.MESSAGE_DIMENSION],
        TGNParameters.NUM_NEIGHBORS: params[TGNParameters.NUM_NEIGHBORS],
        TGNParameters.LAYER_TYPE: get_tgn_layer_enum(params[TGNParameters.LAYER_TYPE]),
        TGNParameters.EDGE_FUNCTION_TYPE: get_edge_message_function_type(params[TGNParameters.EDGE_FUNCTION_TYPE]),
        TGNParameters.MESSAGE_AGGREGATOR_TYPE: get_message_aggregator_type(
            params[TGNParameters.MESSAGE_AGGREGATOR_TYPE]
        ),
        TGNParameters.MEMORY_UPDATER_TYPE: get_memory_updater_type(params[TGNParameters.MEMORY_UPDATER_TYPE]),
    }
    memgraph_objects_property_config = {
        MemgraphObjectsProperties.NODE_FEATURES_PROPERTY: params[MemgraphObjectsProperties.NODE_FEATURES_PROPERTY],
        MemgraphObjectsProperties.EDGE_FEATURES_PROPERTY: params[MemgraphObjectsProperties.EDGE_FEATURES_PROPERTY],
        MemgraphObjectsProperties.NODE_LABELS_PROPERTY: params[MemgraphObjectsProperties.NODE_LABELS_PROPERTY],
    }

    optimizer_config = {
        OptimizerParameters.LEARNING_RATE: params[OptimizerParameters.LEARNING_RATE],
        OptimizerParameters.WEIGHT_DECAY: params[OptimizerParameters.WEIGHT_DECAY],
    }
    # tgn params

    if tgn_config[TGNParameters.LAYER_TYPE] == TGNLayerType.GraphAttentionEmbedding:
        tgn_config[TGNParameters.NUM_ATTENTION_HEADS] = params.get(TGNParameters.NUM_ATTENTION_HEADS, 1)

    # set learning type
    tgn_learning_type = get_learning_type(learning_type)
    tgn_device_type = get_device_type(params[OtherProperties.DEVICE_TYPE])

    set_tgn(
        tgn_learning_type,
        tgn_device_type,
        tgn_config,
        optimizer_config,
        memgraph_objects_property_config,
    )

    return mgp.Record()


#####################################

# helper functions


#####################################
def get_tgn_layer_enum(layer_type: str) -> TGNLayerType:
    if TGNLayerType(layer_type) is TGNLayerType.GraphAttentionEmbedding:
        return TGNLayerType.GraphAttentionEmbedding
    elif TGNLayerType(layer_type) is TGNLayerType.GraphSumEmbedding:
        return TGNLayerType.GraphSumEmbedding
    else:
        raise Exception(
            f"Wrong layer type, expected {TGNLayerType.GraphAttentionEmbedding} "
            f"or {TGNLayerType.GraphSumEmbedding} "
        )


def get_edge_message_function_type(message_function_type: str) -> MessageFunctionType:
    if MessageFunctionType(message_function_type) is MessageFunctionType.Identity:
        return MessageFunctionType.Identity
    elif MessageFunctionType(message_function_type) is MessageFunctionType.MLP:
        return MessageFunctionType.MLP
    else:
        raise Exception(
            f"Wrong message function type, expected {MessageFunctionType.Identity} " f"or {MessageFunctionType.MLP} "
        )


def get_message_aggregator_type(message_aggregator_type: str) -> MessageAggregatorType:
    if MessageAggregatorType(message_aggregator_type) is MessageAggregatorType.Mean:
        return MessageAggregatorType.Mean
    elif MessageAggregatorType(message_aggregator_type) is MessageAggregatorType.Last:
        return MessageAggregatorType.Last
    else:
        raise Exception(
            f"Wrong message aggregator type, expected {MessageAggregatorType.Last} " f"or {MessageAggregatorType.Mean} "
        )


def get_memory_updater_type(memory_updater_type: str) -> MemoryUpdaterType:
    if MemoryUpdaterType(memory_updater_type) is MemoryUpdaterType.GRU:
        return MemoryUpdaterType.GRU

    elif MemoryUpdaterType(memory_updater_type) is MemoryUpdaterType.RNN:
        return MemoryUpdaterType.RNN

    else:
        raise Exception(f"Wrong memory updater type, expected {MemoryUpdaterType.GRU} or" f", {MemoryUpdaterType.RNN}")


def get_learning_type(learning_type: str) -> LearningType:
    if LearningType(learning_type) is LearningType.SelfSupervised:
        return LearningType.SelfSupervised

    elif LearningType(learning_type) is LearningType.Supervised:
        return LearningType.Supervised

    else:
        raise Exception(
            f"Wrong learning type, expected {LearningType.Supervised} or" f", {LearningType.SelfSupervised}"
        )


def get_device_type(device_type: str) -> DeviceType:
    if DeviceType(device_type) is DeviceType.CUDA:
        return DeviceType.CUDA

    elif DeviceType(device_type) is DeviceType.CPU:
        return DeviceType.CPU

    else:
        raise Exception(f"Wrong device type, expected {DeviceType.CUDA} or" f", {DeviceType.CPU}")
