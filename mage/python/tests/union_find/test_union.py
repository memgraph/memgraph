import pytest
from mage.union_find.disjoint_set import DisjointSet
from tests.union_find.constants import Constants


@pytest.fixture
def disjoint_set():
    return DisjointSet(node_ids=Constants.IDs)


class TestUnion:
    def test_equal_height(self, disjoint_set):
        disjoint_set.union(0, 1)
        assert disjoint_set.nodes[1].parent == 0

    def test_different_height(self, disjoint_set):
        disjoint_set.union(0, 1)
        disjoint_set.union(1, 2)
        assert disjoint_set.nodes[2].parent == 0
