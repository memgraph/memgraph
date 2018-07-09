# Distributed addressing

In distributed Memgraph a single graph element must be owned by exactly
one worker. It is possible that multiple workers have cached copies of
a single graph element (which is inevitable), but there is only one
owner.

The owner of a graph element can change. This is not yet implemented,
but is intended. Graph partitioning is intended to be dynamic.

Graph elements refer to other graph elements that are possibly on some
other worker. Even though each graph element is identified with a unique
ID, that ID does not contain the information about where that element
currently resides (which worker is the owner).

Thus we introduce the concept of a global address. It indicates both
which graph element is referred to (it's global ID), and where it
resides. Semantically it's a pair of two elements, but for efficiency
it's stored in 64 bits.

The global address is efficient for usage in a cluster: it indicates
where something can be found. However, finding a graph element based on
it's ID is still not a free operation (in the current implementation
it's a skiplist lookup). So, whenever possible, it's better to use local
addresses (pointers).

Succinctly, the requirements for addressing are:
- global addressing containing location info
- fast local addressing
- storage of both types in the same location efficiently
- translation between the two

The `storage::Address` class handles the enumerated storage
requirements. It stores either a local or global address in the size of
a local pointer (typically 8 bytes).

Conversion between the two is done in multiple places. The general
approach is to use local addresses (when possible) only for local
in-memory handling. All the communication and persistence uses global
addresses. Also, when receiving address from another worker, attempt to
localize addresses as soon as possible, so that least code has to worry
about potential inefficiency of using a global address for a local graph
element.
