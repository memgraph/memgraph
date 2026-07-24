from typing import List

import torch
import torch.nn as nn


class MessageAggregator(nn.Module):
    """
    Base class for all message aggregations
    """

    def __init__(self):
        super().__init__()

    def forward(self, data: List[torch.Tensor]) -> torch.Tensor:
        raise Exception("Not implemented")


class MeanMessageAggregator(MessageAggregator):
    """
    Mean message aggregator
    From messages received as List of torch.Tensor, it creates new aggregated message
    as mean of features received
    """

    def __init__(self):
        super().__init__()

    def forward(self, data: List[torch.Tensor]) -> torch.Tensor:
        # here we will 2D tensor
        # shape = (len(data), num_features)
        data_torch = torch.cat(data)
        # mean across rows
        mean = torch.mean(data_torch, dim=0)
        # return shape = (1, num_features)
        return mean.reshape((1, -1))


class LastMessageAggregator(MessageAggregator):
    """
    Last message aggregator
    From messages received as List of torch.Tensor, it returns last message
    """

    def __init__(self):
        super().__init__()

    def forward(self, data: List[torch.Tensor]) -> torch.Tensor:
        return data[-1]
