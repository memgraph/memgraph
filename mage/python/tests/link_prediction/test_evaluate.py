import pytest
import torch
from mage.link_prediction.constants import Metrics
from mage.link_prediction.link_prediction_util import evaluate


def test_auc_score_single_class_all_positive():
    labels = torch.tensor([1, 1, 1, 1])
    probs = torch.tensor([0.8, 0.6, 0.9, 0.7])
    result = {Metrics.AUC_SCORE: 0.0, Metrics.LOSS: 0.0}

    evaluate(
        metrics=[Metrics.AUC_SCORE],
        labels=labels,
        probs=probs,
        result=result,
        threshold=0.5,
        epoch=1,
        loss=0.1,
        operator=lambda old, new: new,
    )

    assert result[Metrics.AUC_SCORE] == pytest.approx(0.5)


def test_auc_score_single_class_all_negative():
    labels = torch.tensor([0, 0, 0, 0])
    probs = torch.tensor([0.2, 0.3, 0.1, 0.4])
    result = {Metrics.AUC_SCORE: 0.0, Metrics.LOSS: 0.0}

    evaluate(
        metrics=[Metrics.AUC_SCORE],
        labels=labels,
        probs=probs,
        result=result,
        threshold=0.5,
        epoch=1,
        loss=0.1,
        operator=lambda old, new: new,
    )

    assert result[Metrics.AUC_SCORE] == pytest.approx(0.5)


def test_auc_score_both_classes():
    labels = torch.tensor([0, 1, 0, 1])
    probs = torch.tensor([0.2, 0.8, 0.3, 0.9])
    result = {Metrics.AUC_SCORE: 0.0, Metrics.LOSS: 0.0}

    evaluate(
        metrics=[Metrics.AUC_SCORE],
        labels=labels,
        probs=probs,
        result=result,
        threshold=0.5,
        epoch=1,
        loss=0.1,
        operator=lambda old, new: new,
    )

    assert result[Metrics.AUC_SCORE] == pytest.approx(1.0)
