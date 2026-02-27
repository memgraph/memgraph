import random
from collections import defaultdict
from typing import Callable, Dict, List, Tuple

import dgl
import numpy as np
import torch
from mage.link_prediction.constants import Context, Metrics
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score, roc_auc_score

# Function for obtaining reverse_relation naming given original relation
reverse_relation = (
    lambda relation: "rev_" + relation
    if isinstance(relation, str)
    else (relation[2], "rev_" + relation[1], relation[0])
)


def add_self_loop(g: dgl.heterograph, self_loop_edge_type: str) -> dgl.heterograph:
    """Adds self loop to each node with edge type set to self_loop_edge)_type. Creates a new copy of the graph because DGL doesn't support modifying heterograph's
    context.

    Args:
        g (dgl.heterograph): A reference to the original heterograph.
        self_loop_edge_type (str): Name of the self_loop_edge_type.

    Returns:
        dgl.heterograph: New heterograph with added self-loop edges.
    """
    data_dict = dict()
    num_nodes_dict = dict()
    # Copy old edges
    for etype in g.canonical_etypes:
        data_dict[etype] = g.edges(etype=etype)

    # Add self etypes
    device = g.device
    idtype = g.idtype
    for ntype in g.ntypes:
        nids = torch.arange(start=0, end=g.num_nodes(ntype), step=1, dtype=idtype, device=device)
        data_dict[(ntype, self_loop_edge_type, ntype)] = (nids, nids)
        num_nodes_dict[ntype] = g.num_nodes(ntype)

    return dgl.heterograph(data_dict=data_dict, num_nodes_dict=num_nodes_dict, idtype=idtype, device=device)


def proj_0(graph: dgl.graph, node_features_property: str) -> None:
    """Performs projection on all node features to the max_feature_size by padding it with 0.

    Args:
        graph (dgl.graph): A reference to the original graph.
    """
    ftr_size_max = 0
    for node_type in graph.ntypes:  # Not costly, iterates only over node types.
        node_type_features = graph.nodes[node_type].data[node_features_property]
        ftr_size_max = max(ftr_size_max, node_type_features.shape[1])

    for node_type in graph.ntypes:
        p1d = (
            0,
            ftr_size_max - graph.nodes[node_type].data[node_features_property].shape[1],
        )  # Padding left if 0 and padding right is dim_goal - arr.shape[1]

        graph.nodes[node_type].data[node_features_property] = torch.nn.functional.pad(
            graph.nodes[node_type].data[node_features_property],
            p1d,
            mode="constant",
            value=0,
        )


def preprocess(
    graph: dgl.graph, split_ratio: float, target_relation: str, device: torch.device
) -> Tuple[dgl.graph, dgl.graph, dgl.graph, dgl.graph, dgl.graph]:
    """Preprocess method splits dataset in training and validation set by creating necessary masks for distinguishing those two.
        This method is also used for setting numpy and torch random seed.

    Args:
        graph (dgl.graph): A reference to the dgl graph representation.
        split_ratio (float): Split ratio training to validation set. E.g 0.8 indicates that 80% is used as training set and 20% for validation set.
        relation (Tuple[str, str, str]): [src_type, edge_type, dest_type] identifies edges on which model will be trained for prediction
        device (torch.device): Device where the graph is saved

    Returns:
        Tuple[Dict[Tuple[str, str, str], List[int]], Dict[Tuple[str, str, str], List[int]]:
            1. Training mask: target relation to training edge IDs
            2. Validation mask: target relation to validation edge IDS
    """

    # First set all seeds
    rnd_seed = 0
    random.seed(rnd_seed)
    np.random.seed(rnd_seed)
    torch.manual_seed(rnd_seed)  # set it for both cpu and cuda

    # Get edge IDS
    edge_type_u, _ = graph.edges(etype=target_relation)
    graph_edges_len = len(edge_type_u)
    eids = np.arange(graph_edges_len)  # get all edge ids from number of edges and create a numpy vector from it.
    eids = np.random.permutation(eids)  # randomly permute edges
    eids = torch.from_numpy(eids).to(device=device)

    # val size is 1-split_ratio specified by the user
    val_size = int(graph_edges_len * (1 - split_ratio))

    # If user wants to split the dataset but it is too small, then raise an Exception
    if split_ratio < 1.0 and val_size == 0:
        raise Exception("Graph too small to have a validation dataset. ")

    # Get training and validation edges
    tr_eids, val_eids = eids[val_size:], eids[:val_size]

    # Create and masks that will be used in the batch training
    train_eid_dict, val_eid_dict = {target_relation: tr_eids}, {target_relation: val_eids}

    return train_eid_dict, val_eid_dict


def classify(probs: torch.tensor, threshold: float) -> torch.tensor:
    """Classifies based on probabilities of the class with the label one.

    Args:
        probs (torch.tensor): Edge probabilities.

    Returns:
        torch.tensor: classes
    """

    return probs > threshold


def evaluate(
    metrics: List[str],
    labels: torch.tensor,
    probs: torch.tensor,
    result: Dict[str, float],
    threshold: float,
    epoch: int,
    loss: float,
    operator: Callable[[float, float], float],
) -> None:
    """Returns all metrics specified in metrics list based on labels and predicted classes. In-place modification of dictionary.

    Args:
        metrics (List[str]): List of string metrics.
        labels (torch.tensor): Predefined labels.
        probs (torch.tensor): Probabilities of src_nodes = blocks[0].srcdata[dgl.NID]
    dst_nodes = blocks[0].dstdata[dgl.NID]
    Returns:
        Dict[str, float]: Metrics embedded in dictionary -> name-value shape
    """
    labels = labels.detach().cpu()
    probs = probs.detach().cpu()
    classes = classify(probs, threshold)
    result[Metrics.EPOCH] = epoch
    result[Metrics.LOSS] = operator(result[Metrics.LOSS], loss)
    tn, fp, fn, tp = confusion_matrix(labels, classes).ravel()
    for metric_name in metrics:
        if metric_name == Metrics.ACCURACY:
            result[Metrics.ACCURACY] = operator(result[Metrics.ACCURACY], accuracy_score(labels, classes))
        elif metric_name == Metrics.AUC_SCORE:
            if labels.max() == labels.min():
                auc = 0.5
            else:
                auc = roc_auc_score(labels, probs.detach())
            result[Metrics.AUC_SCORE] = operator(result[Metrics.AUC_SCORE], auc)
        elif metric_name == Metrics.F1:
            result[Metrics.F1] = operator(result[Metrics.F1], f1_score(labels, classes))
        elif metric_name == Metrics.PRECISION:
            result[Metrics.PRECISION] = operator(result[Metrics.PRECISION], precision_score(labels, classes))
        elif metric_name == Metrics.RECALL:
            result[Metrics.RECALL] = operator(result[Metrics.RECALL], recall_score(labels, classes))
        elif metric_name == Metrics.POS_PRED_EXAMPLES:
            result[Metrics.POS_PRED_EXAMPLES] = operator(result[Metrics.POS_PRED_EXAMPLES], classes.sum().item())
        elif metric_name == Metrics.NEG_PRED_EXAMPLES:
            result[Metrics.NEG_PRED_EXAMPLES] = operator(result[Metrics.NEG_PRED_EXAMPLES], classes.sum().item())
        elif metric_name == Metrics.POS_EXAMPLES:
            result[Metrics.POS_EXAMPLES] = operator(result[Metrics.POS_EXAMPLES], (labels == 1).sum().item())
        elif metric_name == Metrics.NEG_EXAMPLES:
            result[Metrics.NEG_EXAMPLES] = operator(result[Metrics.NEG_EXAMPLES], (labels == 0).sum().item())
        elif metric_name == Metrics.TRUE_POSITIVES:
            result[Metrics.TRUE_POSITIVES] = operator(result[Metrics.TRUE_POSITIVES], tp)
        elif metric_name == Metrics.FALSE_POSITIVES:
            result[Metrics.FALSE_POSITIVES] = operator(result[Metrics.FALSE_POSITIVES], fp)
        elif metric_name == Metrics.TRUE_NEGATIVES:
            result[Metrics.TRUE_NEGATIVES] = operator(result[Metrics.TRUE_NEGATIVES], tn)
        elif metric_name == Metrics.FALSE_NEGATIVES:
            result[Metrics.FALSE_NEGATIVES] = operator(result[Metrics.FALSE_NEGATIVES], fn)


def batch_forward_pass(
    model: torch.nn.Module,
    predictor: torch.nn.Module,
    loss: torch.nn.Module,
    m: torch.nn.Module,
    target_relation: str,
    input_features: Dict[str, torch.Tensor],
    pos_graph: dgl.graph,
    neg_graph: dgl.graph,
    blocks: List[dgl.graph],
    num_neg_per_pos_edge: int,
    device: torch.device,
) -> Tuple[torch.Tensor, torch.Tensor, torch.nn.Module]:
    """Performs one forward batch pass

    Args:
        model (torch.nn.Module): A reference to the model that needs to be trained.
        predictor (torch.nn.Module): A reference to the edge predictor.
        loss (torch.nn.Module): Loss function.
        m (torch.nn.Module): The activation function.
        target_relation: str -> Unique edge type that is used for training.
        input_features (Dict[str, torch.Tensor]): A reference to the input_features that are needed to compute representations for second block.
        pos_graph (dgl.graph): A reference to the positive graph. All edges that should be included.
        neg_graph (dgl.graph): A reference to the negative graph. All edges that shouldn't be included.
        blocks (List[dgl.graph]): First DGLBlock(MFG) is equivalent to all necessary nodes that are needed to compute final representation.
            Second DGLBlock(MFG) is a mini-batch.
        device (torch.device): Device where the graph is saved.

    Returns:
         Tuple[torch.Tensor, torch.Tensor, torch.nn.Module]: First tensor are calculated probabilities, second tensor are true labels and the last tensor
            is a reference to the loss.
    """
    outputs = model.forward(blocks, input_features)
    # Deal with edge scores
    pos_score = predictor.forward(pos_graph, outputs, target_relation=target_relation)
    neg_score = predictor.forward(neg_graph, outputs, target_relation=target_relation)
    scores = torch.cat([pos_score, neg_score])  # concatenated positive and negative score
    # probabilities
    probs = m(scores)
    labels = torch.cat(
        [
            torch.ones(pos_score.shape[0], device=device),
            torch.zeros(neg_score.shape[0], device=device),
        ]
    )  # concatenation of labels
    # weights = torch.cat([torch.ones(pos_score.shape[0], dtype=torch.float32), torch.Tensor([1.0 / num_neg_per_pos_edge for _ in range(neg_score.shape[0])])])
    loss_output = loss(probs, labels)

    return probs, labels, loss_output


def inner_train(
    graph: dgl.graph,
    train_eid_dict,
    val_eid_dict,
    target_relation: str,
    model: torch.nn.Module,
    predictor: torch.nn.Module,
    optimizer: torch.optim.Optimizer,
    num_epochs: int,
    m: torch.nn.Module,
    threshold: float,
    node_features_property: str,
    console_log_freq: int,
    checkpoint_freq: int,
    metrics: List[str],
    tr_acc_patience: int,
    context_save_dir: str,
    num_neg_per_pos_edge: int,
    num_layers: int,
    batch_size: int,
    sampling_workers: int,
    device: torch.device,
) -> Tuple[List[Dict[str, float]], torch.nn.Module, torch.Tensor]:
    """Batch training method.

    Args:
        graph (dgl.graph): A reference to the original graph.
        train_eid_dict (_type_): Mask that identifies training part of the graph. This included only edges from a given relation.
        val_eid_dict (_type_): Mask that identifies validation part of the graph. This included only edges from a given relation.
        target_relation: str -> Unique edge type that is used for training.
        model (torch.nn.Module): A reference to the model that will be trained.
        predictor (torch.nn.Module): A reference to the edge predictor.
        optimizer (torch.optim.Optimizer): A reference to the training optimizer.
        num_epochs (int): number of epochs for model training.
        m (torch.nn.Module): Activation function.
        threshold (float): Classification threshold for given activation function.
        node_features_property: (str): property name where the node features are saved.
        console_log_freq (int): How often results will be printed. All results that are printed in the terminal will be returned to the client calling Memgraph.
        checkpoint_freq (int): Select the number of epochs on which the model will be saved. The model is persisted on the disc.
        metrics (List[str]): Metrics used to evaluate model in training on the validation set.
            Epoch will always be displayed, you can add loss, accuracy, precision, recall, specificity, F1, auc_score etc.
        tr_acc_patience (int): Training patience, for how many epoch will accuracy drop on validation set be tolerated before stopping the training.
        context_save_dir (str): Path where the model and predictor will be saved every checkpoint_freq epochs.
        num_neg_per_pos_edge (int): Number of negative edges that will be sampled per one positive edge in the mini-batch.
        num_layers (int): Number of layers in the GNN architecture.
        batch_size (int): Batch size used in both training and validation procedure.
        sampling_workers (int): Number of workers that will cooperate in the sampling procedure in the training and validation.
        device (torch.device): cpu or cuda
    Returns:
        Tuple[List[Dict[str, float]], torch.nn.Module, torch.Tensor]: Training and validation results. _
    """
    # Define what will be returned
    training_results, validation_results = [], []

    # First define all necessary samplers
    negative_sampler = dgl.dataloading.negative_sampler.GlobalUniform(k=num_neg_per_pos_edge, replace=False)
    sampler = dgl.dataloading.MultiLayerFullNeighborSampler(
        num_layers=num_layers, output_device=device
    )  # gather messages from all node neighbors

    # Create reverse target relation
    reverse_target_relation = reverse_relation(target_relation)
    if reverse_target_relation not in graph.etypes and reverse_target_relation not in graph.canonical_etypes:
        # same source and destination node
        sampler = dgl.dataloading.as_edge_prediction_sampler(sampler, negative_sampler=negative_sampler, exclude="self")
    else:
        reverse_etypes = {
            target_relation: reverse_target_relation,
            reverse_target_relation: target_relation,
        }
        sampler = dgl.dataloading.as_edge_prediction_sampler(
            sampler,
            negative_sampler=negative_sampler,
            exclude="reverse_types",
            reverse_etypes=reverse_etypes,
        )

    # Define training and validation dictionaries
    # For heterogeneous full neighbor sampling we need to define a dictionary of edge types and edge ID tensors instead of a dictionary of node types and node ID tensors
    # DataLoader iterates over a set of edges in mini-batches, yielding the subgraph induced by the edge mini-batch and message flow graphs (MFGs) to be consumed by the module below.
    # first MFG, which is identical to all the necessary nodes needed for computing the final representations
    # Feed the list of MFGs and the input node features to the multilayer GNN and get the outputs.

    # Define training EdgeDataLoader
    train_dataloader = dgl.dataloading.DataLoader(
        graph,  # The graph
        train_eid_dict,  # The edges to iterate over
        sampler,  # The neighbor sampler
        device=device,
        batch_size=batch_size,  # Batch size
        shuffle=True,  # Whether to shuffle the nodes for every epoch
        drop_last=False,  # Whether to drop the last incomplete batch
        num_workers=sampling_workers,  # Number of sampling processes
    )

    # Define validation EdgeDataLoader
    validation_dataloader = dgl.dataloading.DataLoader(
        graph,  # The graph
        val_eid_dict,  # The edges to iterate over
        sampler,  # The neighbor sampler
        device=device,
        batch_size=batch_size,  # Batch size
        shuffle=True,  # Whether to shuffle the nodes for every epoch
        drop_last=False,  # Whether to drop the last incomplete batch
        num_workers=sampling_workers,  # Number of sampler processes
    )

    loss = torch.nn.BCELoss()

    # Define lambda functions for operating on dictionaries
    add_: Callable[[float, float], float] = lambda prior, later: prior + later
    avg_: Callable[[float, float], float] = lambda prior, size: prior / size
    format_float: Callable[[float], float] = lambda prior: round(prior, 3)

    # Training
    max_val_acc, num_val_acc_drop = (
        -1.0,
        0,
    )  # last maximal accuracy and number of epochs it is dropping

    for epoch in range(1, num_epochs + 1):
        # Evaluation epoch
        if epoch % console_log_freq == 0:
            epoch_training_result = defaultdict(float)
            epoch_validation_result = defaultdict(float)
        # Training batch
        num_batches = 0
        model.train()
        tr_finished = False
        for _, pos_graph, neg_graph, blocks in train_dataloader:
            input_features = blocks[0].ndata[node_features_property]
            # Perform forward pass
            probs, labels, loss_output = batch_forward_pass(
                model,
                predictor,
                loss,
                m,
                target_relation,
                input_features,
                pos_graph,
                neg_graph,
                blocks,
                num_neg_per_pos_edge,  # TODO: remove
                device,
            )
            # Make an optimization step
            optimizer.zero_grad()
            loss_output.backward()  # ***This line generates warning***
            optimizer.step()
            # Evaluate on training set
            if epoch % console_log_freq == 0:
                evaluate(
                    metrics,
                    labels,
                    probs,
                    epoch_training_result,
                    threshold,
                    epoch,
                    loss_output.item(),
                    add_,
                )
            # Increment num batches
            num_batches += 1
        # Edit train results and evaluate on validation set
        if epoch % console_log_freq == 0:
            epoch_training_result = {
                key: format_float(avg_(val, num_batches)) if key != Metrics.EPOCH else val
                for key, val in epoch_training_result.items()
            }
            training_results.append(epoch_training_result)
            # Check if training finished
            if Metrics.ACCURACY in metrics and epoch_training_result[Metrics.ACCURACY] == 1.0 and epoch > 1:
                tr_finished = True
            # Evaluate on the validation set
            model.eval()
            with torch.no_grad():
                num_batches = 0
                for _, pos_graph, neg_graph, blocks in validation_dataloader:
                    input_features = blocks[0].ndata[node_features_property]
                    # Perform forward pass
                    probs, labels, loss_output = batch_forward_pass(
                        model,
                        predictor,
                        loss,
                        m,
                        target_relation,
                        input_features,
                        pos_graph,
                        neg_graph,
                        blocks,
                        num_neg_per_pos_edge,  # TODO: remove
                        device,
                    )
                    # Add to the epoch_validation_result for saving
                    evaluate(
                        metrics,
                        labels,
                        probs,
                        epoch_validation_result,
                        threshold,
                        epoch,
                        loss_output.item(),
                        add_,
                    )
                    num_batches += 1
            if num_batches > 0:  # Because it is possible that user specified not to have a validation dataset
                # Average over batches
                epoch_validation_result = {
                    key: format_float(avg_(val, num_batches)) if key != Metrics.EPOCH else val
                    for key, val in epoch_validation_result.items()
                }
                validation_results.append(epoch_validation_result)
                if (
                    Metrics.ACCURACY in metrics
                ):  # If user doesn't want to have accuracy information, it cannot be checked for patience.
                    # Patience check
                    if epoch_validation_result[Metrics.ACCURACY] <= max_val_acc:
                        num_val_acc_drop += 1
                    else:
                        max_val_acc = epoch_validation_result[Metrics.ACCURACY]
                        num_val_acc_drop = 0
                    # Stop the training if necessary
                    if num_val_acc_drop == tr_acc_patience:
                        break

        # Save the model if necessary
        if epoch % checkpoint_freq == 0:
            _save_context(model, predictor, context_save_dir)
        # All examples learnt
        if tr_finished:
            break

    # Save model at the end of the training
    _save_context(model, predictor, context_save_dir)

    return training_results, validation_results


def _save_context(model: torch.nn.Module, predictor: torch.nn.Module, context_save_dir: str):
    """Saves model and predictor to path.

    Args:
        context_save_dir: str -> Path where the model and predictor will be saved every checkpoint_freq epochs.
        model (torch.nn.Module): A reference to the model.
        predictor (torch.nn.Module): A reference to the predictor.
    """
    torch.save(model, context_save_dir + Context.MODEL_NAME)
    torch.save(predictor, context_save_dir + Context.PREDICTOR_NAME)


def inner_predict(
    model: torch.nn.Module,
    predictor: torch.nn.Module,
    graph: dgl.graph,
    node_features_property: str,
    src_node: int,
    dest_node: int,
    src_type=str,
    dest_type=str,
) -> float:
    """Predicts edge scores for given graph. This method is called to obtain edge probability for edge with id=edge_id.

    Args:
        model (torch.nn.Module): A reference to the trained model.
        predictor (torch.nn.Module): A reference to the predictor.
        graph (dgl.graph): A reference to the graph. This is semi-inductive setting so new nodes are appended to the original graph(train+validation).
        node_features_property (str): Property name of the features.
        src_node (int): Source node of the edge.
        dest_node (int): Destination node of the edge.
        src_type (str): Type of the source node.
        dest_type (str): Type of the destination node.

    Returns:
        float: Edge score.
    """
    graph_features = {node_type: graph.nodes[node_type].data[node_features_property] for node_type in graph.ntypes}
    with torch.no_grad():
        h = model.online_forward(graph, graph_features)
        src_embedding, dest_embedding = h[src_type][src_node], h[dest_type][dest_node]
        score = predictor.forward_pred(src_embedding, dest_embedding)
        prob = torch.sigmoid(score)
        return prob.item()
