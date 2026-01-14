from typing import Tuple

import torch
import torch.nn as nn


class MemoryUpdater(nn.Module):
    """
    This is base class for memory updater implementation
    """

    def __init__(self, memory_dimension: int, message_dimension: int, device: torch.device):
        super().__init__()
        self.memory_dimension = memory_dimension
        self.message_dimension = message_dimension
        self.device = device


class MemoryUpdaterGRU(MemoryUpdater):
    def __init__(self, memory_dimension: int, message_dimension: int, device: torch.device):
        super().__init__(memory_dimension, message_dimension, device)

        self.memory_updater_net = nn.GRUCell(input_size=message_dimension, hidden_size=memory_dimension).to(self.device)

    def forward(self, data: Tuple[torch.Tensor, torch.Tensor]):
        # messages shape = (1, message_dim)
        # memory shape = (memory_dim,)
        messages, memory = data

        # memory_dim = (1, memory_dim)
        memory = memory.unsqueeze(0)

        return self.memory_updater_net(messages, memory)


class MemoryUpdaterRNN(MemoryUpdater):
    def __init__(self, memory_dimension: int, message_dimension: int, device: torch.device):
        super().__init__(memory_dimension, message_dimension, device)

        self.memory_updater_net = nn.RNNCell(input_size=message_dimension, hidden_size=memory_dimension).to(self.device)

    def forward(self, data: Tuple[torch.Tensor, torch.Tensor]):
        # messages shape = (1, message_dim)
        # memory shape = (memory_dim,)
        messages, memory = data

        # memory_dim = (1, memory_dim)
        memory = memory.unsqueeze(0)

        return self.memory_updater_net(messages, memory)
