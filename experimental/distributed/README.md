
# distributed memgraph

This subdirectory structure implements distributed infrastructure of Memgraph.

## Terminology

* Memgraph Node Id (mnid): a machine (processs) that runs a (distributed) Memgraph program.
* Node: a computer that performs (distributed) work.
* Vertex: an abstract graph concept.
* Reactor: a unit of concurrent execution, lives on its own thread.
* Channel: a (one-way) communication abstraction between Reactors. The reactors can be on the same machine or on different processes.
* Message: gets sent through channels. Must be serializable if sent via network layer (library: cereal).
* Event: arrival of a (subclass of) Message. You can register callbacks. Register exact callbacks (not for derivated/subclasses).
* EventStream: read-end of a channel, is owned by exactly one Reactor/thread.
* ChannelWriter: write-end of a channel, can be owned (wrote into) by multiple threads.

## Ownership:

* System, Distributed are singletons. They should be always alive.
* ChannelWriter (write-end) should be lightweight and can be copied arbitrarily.
* EventStream (read-end) should never be written by anyone except the owner (the reactor that created it).

## Code Conventions

* Locked: A method having a Locked... prefix indicates that you
  have to lock the appropriate mutex before calling this function.
* ALWAYS close channels. You will memory leak if you don't.
  Reactor::CloseChannel or Subscription::Close will do the trick.

## Dependencies

* cereal
* <other memgraph dependencies>