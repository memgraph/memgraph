import itertools
from typing import List, Tuple

import torch
from mage.link_prediction.constants import Activations, Models, Optimizers, Predictors
from mage.link_prediction.models.gat import GAT
from mage.link_prediction.models.graph_sage import GraphSAGE
from mage.link_prediction.predictors.DotPredictor import DotPredictor
from mage.link_prediction.predictors.MLPPredictor import MLPPredictor


def create_optimizer(
    optimizer_type: str,
    learning_rate: float,
    model: torch.nn.Module,
    predictor: torch.nn.Module,
) -> torch.optim.Optimizer:
    """Creates optimizer with given optimizer type and learning rate.

    Args:
        optimizer_type (str): Type of the optimizer.
        learning_rate (float): Learning rate of the optimizer.
        model (torch.nn.Module): A reference to the already created model. Needed to chain the parameters.
        predictor: (torch.nn.Module): A reference to the already created predictor. Needed to chain the parameters.
    Returns:
        torch.nn.Optimizer: Optimizer used in the training.
    """
    optimizer_jump_table = {
        Optimizers.ADAM_OPT: torch.optim.Adam,
        Optimizers.SGD_OPT: torch.optim.SGD,
    }
    optimizer_type = optimizer_type.upper()
    if optimizer_type in optimizer_jump_table:
        return optimizer_jump_table[optimizer_type](
            itertools.chain(model.parameters(), predictor.parameters()),
            lr=learning_rate,
        )
    else:
        raise Exception(f"Optimizer {optimizer_type} not supported. ")


def create_model(
    layer_type: str,
    in_feats: int,
    hidden_features_size: List[int],
    aggregator: str,
    attn_num_heads: List[int],
    feat_drops: List[float],
    attn_drops: List[float],
    alphas: List[float],
    residuals: List[bool],
    edge_types: List[str],
    device: torch.device,
) -> torch.nn.Module:
    """Creates a model given a layer type and sizes of the hidden layers.

    Args:
        layer_type (str): Layer type.
        in_feats (int): Defines the size of the input features.
        hidden_features_size (List[int]): Defines the size of each hidden layer in the architecture.
        aggregator str: Type of the aggregator that will be used in GraphSage. Ignored for GAT.
        attn_num_heads List[int] : Number of heads for each layer used in the graph attention network. Ignored for GraphSage.
        feat_drops List[float]: Dropout rate on feature for each layer.
        attn_drops List[float]: Dropout rate on attention weights for each layer. Used only in GAT.
        alphas List[float]: LeakyReLU angle of negative slope for each layer. Used only in GAT.
        residuals List[bool]: Use residual connection for each layer or not. Used only in GAT.
        edge_types (List[str]): All edge types that are occurring in the heterogeneous network.

    Returns:
        torch.nn.Module: Model used in the link prediction task.
    """
    if layer_type.lower() == Models.GRAPH_SAGE:
        return GraphSAGE(
            in_feats=in_feats,
            hidden_features_size=hidden_features_size,
            aggregator=aggregator,
            feat_drops=feat_drops,
            edge_types=edge_types,
            device=device,
        )
    elif layer_type.lower() == Models.GRAPH_ATTN:
        return GAT(
            in_feats=in_feats,
            hidden_features_size=hidden_features_size,
            attn_num_heads=attn_num_heads,
            feat_drops=feat_drops,
            attn_drops=attn_drops,
            alphas=alphas,
            residuals=residuals,
            edge_types=edge_types,
            device=device,
        )
    else:
        raise Exception(f"Layer type {layer_type} not supported. ")


def create_predictor(predictor_type: str, predictor_hidden_size: int, device: torch.device) -> torch.nn.Module:
    """Create a predictor based on a given predictor type.

    Args:
        predictor_type (str): Name of the predictor.
        predictor_hidden_size (int): Size of the hidden layer in MLP predictor. It will only be used for the MLPPredictor.
    Returns:
        torch.nn.Module: Predictor implemented in predictors module.
    """
    if predictor_type.lower() == Predictors.DOT_PREDICTOR:
        return DotPredictor()
    elif predictor_type.lower() == Predictors.MLP_PREDICTOR:
        return MLPPredictor(h_feats=predictor_hidden_size, device=device)
    else:
        raise Exception(f"Predictor type {predictor_type} not supported. ")


def create_activation_function(act_func: str) -> Tuple[torch.nn.Module, float]:
    """Creates activation function based on a given name.

    Args:
        act_func (str): Name of the activation function.

    Returns:
        torch.nn.Module: Created activation function
        float: Classification threshold
    """
    if act_func == Activations.SIGMOID:
        return torch.nn.Sigmoid(), 0.5
    else:
        raise Exception(f"Currently, only {Activations.SIGMOID} is supported. ")
