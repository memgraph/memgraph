import os

import dgl
import pytest
import torch
from mage.link_prediction.constants import Context
from mage.link_prediction.factory import create_model, create_predictor
from mage.link_prediction.link_prediction_util import load_context, save_context


def make_config(layer_type, predictor_type):
    return {
        "model": {
            "layer_type": layer_type,
            "in_feats": 8,
            "hidden_features_size": [16, 16],
            "aggregator": "pool",
            "attn_num_heads": [2, 1],
            "feat_drops": [0.1, 0.1],
            "attn_drops": [0.1, 0.1],
            "alphas": [0.5, 0.5],
            "residuals": [True, True],
            "edge_types": ["LINK"],
        },
        "predictor": {"predictor_type": predictor_type, "predictor_hidden_size": 16},
    }


def save(config, save_dir):
    model = create_model(**config["model"], device=None)
    predictor = create_predictor(**config["predictor"], device=None)
    save_context(model, predictor, config, save_dir)
    return model, predictor


@pytest.mark.parametrize("layer_type, predictor_type", [("graph_attn", "mlp"), ("graph_sage", "dot")])
def test_load_context_restores_weights(tmp_path, layer_type, predictor_type):
    model, predictor = save(make_config(layer_type, predictor_type), str(tmp_path))

    loaded_model, loaded_predictor = load_context(str(tmp_path), None)

    for saved, loaded in ((model, loaded_model), (predictor, loaded_predictor)):
        saved_state, loaded_state = saved.state_dict(), loaded.state_dict()
        assert saved_state.keys() == loaded_state.keys()
        assert all(torch.equal(saved_state[k], loaded_state[k]) for k in saved_state)

    torch.manual_seed(0)
    graph = dgl.heterograph({("Node", "LINK", "Node"): (torch.tensor([0, 1, 2]), torch.tensor([1, 2, 0]))})
    features = {"Node": torch.rand(3, 8)}
    model.eval()
    loaded_model.eval()
    assert torch.equal(
        model.online_forward(graph, features)["Node"], loaded_model.online_forward(graph, features)["Node"]
    )


def test_load_context_path_without_trailing_slash(tmp_path):
    save_dir = os.path.join(str(tmp_path), "context")
    os.mkdir(save_dir)
    save(make_config("graph_sage", "mlp"), save_dir)

    assert sorted(os.listdir(save_dir)) == sorted([Context.CONFIG_NAME, Context.MODEL_NAME, Context.PREDICTOR_NAME])
    load_context(save_dir, None)


def test_load_context_old_format(tmp_path):
    # Older versions pickled the whole model and predictor and saved no config.
    config = make_config("graph_sage", "mlp")
    torch.save(create_model(**config["model"], device=None), os.path.join(str(tmp_path), Context.MODEL_NAME))
    torch.save(
        create_predictor(**config["predictor"], device=None), os.path.join(str(tmp_path), Context.PREDICTOR_NAME)
    )

    with pytest.raises(Exception, match="need to be trained again"):
        load_context(str(tmp_path), None)
