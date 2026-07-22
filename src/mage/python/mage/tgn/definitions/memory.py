from typing import Dict

import torch


class Memory:
    def __init__(self, memory_dimension: int, device: torch.device):
        self.memory_dimension = memory_dimension
        self.device = device
        self.init_memory()

    def init_memory(self):
        self.memory_container: Dict[int, torch.Tensor] = {}
        self.last_node_update: Dict[int, torch.Tensor] = {}

    # https://stackoverflow.com/questions/48274929/pytorch-runtimeerror-trying-to-backward-through-the-graph-a-second
    # -time-but
    def detach_tensor_grads(self):
        for node_memory in self.memory_container.values():
            if node_memory.grad is not None:
                node_memory.grad.zero_()

        for timestamp in self.last_node_update.values():
            if timestamp.grad is not None:
                timestamp.grad.zero_()

    def get_node_memory(self, node: int) -> torch.Tensor:
        if node not in self.memory_container:
            self.memory_container[node] = torch.zeros(
                self.memory_dimension,
                dtype=torch.float32,
                device=self.device,
                requires_grad=True,
            )
        return self.memory_container[node]

    def set_node_memory(self, node: int, node_memory: torch.Tensor) -> torch.Tensor:
        self.memory_container[node] = node_memory
        return self.memory_container[node]

    def get_last_node_update(self, node: int) -> torch.Tensor:
        if node not in self.last_node_update:
            self.last_node_update[node] = torch.zeros(1, dtype=torch.float32, device=self.device, requires_grad=True)
        return self.last_node_update[node]
