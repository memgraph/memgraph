import numpy as np
import torch
import torch.nn as nn


# time encoding by GAT
class TimeEncoder(nn.Module):
    def __init__(self, out_dimension: int, device: torch.device):
        super().__init__()
        self.device = device
        self.out_dimension = out_dimension
        self.w = nn.Linear(1, out_dimension).to(self.device)

        self.w.weight = nn.Parameter(
            (torch.from_numpy(1 / 10 ** np.linspace(0, 9, out_dimension))).float().reshape(out_dimension, -1)
        )
        self.w.bias = nn.Parameter(torch.zeros(out_dimension, device=self.device).float())

    def forward(self, t):
        output = torch.cos(self.w(t))
        return output
