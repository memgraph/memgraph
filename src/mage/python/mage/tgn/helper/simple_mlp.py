from typing import List

import torch.nn as nn


class MLP(nn.Module):
    def __init__(self, dims: List[int]):
        super().__init__()
        assert len(dims) == 3
        self.fc1 = nn.Linear(dims[0], dims[1])
        self.fc2 = nn.Linear(dims[1], dims[2])
        self.act = nn.ReLU(inplace=False)

        nn.init.xavier_normal_(self.fc1.weight)
        nn.init.xavier_normal_(self.fc2.weight)

    def forward(self, data):
        h = self.act(self.fc1(data))
        return self.fc2(h)


class SimpleMLP(nn.Module):
    def __init__(self, dim_input, dim_output):
        super(SimpleMLP, self).__init__()
        self.fc1 = nn.Linear(dim_input, dim_input // 2)
        self.fc2 = nn.Linear(dim_input // 2, dim_output)
        self.act = nn.ReLU(inplace=False)

        nn.init.xavier_normal_(self.fc1.weight)
        nn.init.xavier_normal_(self.fc2.weight)

    def forward(self, data):
        h = self.act(self.fc1(data))
        return self.fc2(h).squeeze(dim=0)
