import pytest
from mage.union_find.disjoint_set import DisjointSet
from tests.union_find.constants import Constants


@pytest.fixture
def disjoint_set():
    return DisjointSet(node_ids=Constants.IDs)


class TestInit:
    def test_keys(self, disjoint_set):
        assert all(i in disjoint_set.nodes.keys() for i in Constants.IDs)

    def test_parent(self, disjoint_set):
        assert all(ID == disjoint_set.nodes[ID].parent for ID in Constants.IDs)

    def test_rank(self, disjoint_set):
        assert all(disjoint_set.nodes[ID].rank == 0 for ID in Constants.IDs)
