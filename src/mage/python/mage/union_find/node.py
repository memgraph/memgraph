INITIAL_RANK = 0


class Node:
    """
    Class implementing a node in an union-find data structure.
    Stores the current node's rank and a reference to its parent node.
    """

    def __init__(self, parent_id: int, rank: int = INITIAL_RANK):
        self._parent = parent_id
        self._rank = rank

    @property
    def parent(self) -> int:
        return self._parent

    @parent.setter
    def parent(self, x: int):
        self._parent = x

    @property
    def rank(self) -> int:
        return self._rank

    @rank.setter
    def rank(self, x: int):
        self._rank = x
