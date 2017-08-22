
# distributed memgraph

This subdirectory structure implements distributed infrastructure of Memgraph.

## terminology

* Memgraph Node Id (mnid): a machine (processs) that runs a (distributed) Memgraph program.
* Node: a computer that performs (distributed) work.
* Vertex: an abstract graph concept.
* Reactor: a unit of concurrent execution, lives on its own thread.
* Connector: a (one-way) communication abstraction between Reactors. The reactors can be on the same machine or on different processes.
* EventStream: read-end of a connector, is owned by exactly one Reactor/thread.
* Channel: write-end of a connector, can be owned (wrote into) by multiple threads.

## conventions

1. Locked: A method having a Locked... prefix indicates that you
have to lock the appropriate mutex before calling this function.

## dependencies

* cereal
* <other memgraph dependencies>