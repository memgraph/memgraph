import pytest
from mage.union_find.disjoint_set import DisjointSet
from tests.union_find.constants import Constants


@pytest.fixture
def disjoint_set():
    return DisjointSet(node_ids=Constants.IDs)


class TestFind:
    def test_disconnected(self, disjoint_set):
        assert disjoint_set.connected(0, 1) is False

    def test_connected(self, disjoint_set):
        disjoint_set.union(0, 1)
        assert disjoint_set.connected(0, 1) is True

    def test_connected_transitivity(self, disjoint_set):
        disjoint_set.union(0, 1)
        disjoint_set.union(1, 2)
        assert disjoint_set.connected(0, 2) is True
