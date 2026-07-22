from collections import defaultdict
from typing import Dict, List

from mage.tgn.definitions.messages import RawMessage


class RawMessageStore:
    """
    This class represents store for instances of Raw Messages
    """

    def __init__(self, edge_raw_message_dimension: int, node_raw_message_dimension: int):
        self.edge_raw_message_dimension = edge_raw_message_dimension
        self.node_raw_message_dimension = node_raw_message_dimension
        self.init_message_store()

    def init_message_store(self) -> None:
        self.message_container: Dict[int, List[RawMessage]] = defaultdict(list)

    def detach_grads(self) -> None:
        for messages in self.message_container.values():
            for message in messages:
                message.detach_memory()

    def get_messages(self) -> Dict[int, List[RawMessage]]:
        return self.message_container

    def update_messages(self, new_node_messages: Dict[int, List[RawMessage]]) -> None:
        for node in new_node_messages:
            self.message_container[node].extend(new_node_messages[node])
