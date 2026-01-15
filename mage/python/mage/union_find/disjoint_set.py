from typing import Dict, List

from mage.union_find.node import INITIAL_RANK, Node


class DisjointSet:
    """
    Class implementing a disjoint-set data structure.
    """

    def __init__(self, node_ids: List[int] = None):
        if node_ids is None:
            self.nodes: Dict[int, Node] = {}
        else:
            self.nodes: Dict[int, Node] = {node_id: Node(parent_id=node_id, rank=INITIAL_RANK) for node_id in node_ids}

    def reinitialize(self, node_ids: List[int]):
        """
        Reinitializes the data structure.

        :param node_ids: NodeIDs to be included in the structure.
        :type node_ids: List[int]
        """
        self.nodes.clear()
        self.nodes: Dict[int, Node] = {node_id: Node(parent_id=node_id, rank=INITIAL_RANK) for node_id in node_ids}

    def parent(self, node_id: int) -> int:
        """
        Returns given node's parent's ID.

        :param node_id: Node ID
        :type node_id: int
        """
        return self.nodes[node_id].parent

    def grandparent(self, node_id: int) -> int:
        """
        Returns given node's grandparent's ID.

        :param node_id: Node ID
        :type node_id: int
        """
        return self.parent(self.parent(node_id))

    def rank(self, node_id: int) -> int:
        """
        Returns given node's rank.

        :param node_id: Node ID
        :type node_id: int
        """
        return self.nodes[node_id].rank

    def find(self, node_id: int) -> int:
        """
        Returns the representative node's ID for the component that given node is member of.
        Uses path splitting (https://en.wikipedia.org/wiki/Disjoint-set_data_structure#Finding_set_representatives)
        in order to keep trees representing connected components flat.

        :param node_id: Node ID
        :type node_id: int
        """
        while node_id != self.parent(node_id):
            self.nodes[node_id].parent = self.grandparent(node_id)  # path splitting
            node_id = self.parent(node_id)

        return node_id

    def union(self, node1_id: int, node2_id: int) -> None:
        """
        Unites the components containing two given nodes. Implements union by rank to reduce component tree height.

        :param node1_id: First node's ID
        :type node1_id: int
        :param node2_id: Second node's ID
        :type node2_id: int
        """
        root_1 = self.find(node1_id)
        root_2 = self.find(node2_id)

        if root_1 == root_2:
            return

        if self.rank(root_1) < self.rank(root_2):
            root_1, root_2 = root_2, root_1

        self.nodes[root_2].parent = root_1
        if self.rank(root_1) == self.rank(root_2):
            self.nodes[root_1].rank = self.rank(root_1) + 1

    def connected(self, node1_id: int, node2_id: int) -> bool:
        """
        Returns whether two given nodes belong to the same connected component.

        :param node1_id: First node's ID
        :type node1_id: int
        :param node2_id: Second node's ID
        :type node2_id: int
        """
        return self.find(node1_id) == self.find(node2_id)
