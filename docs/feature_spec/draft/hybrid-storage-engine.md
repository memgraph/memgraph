# Hybrid Storage Engine

The goal here is easy to improve Memgraph storage massively! Please take a look
[here](http://cidrdb.org/cidr2020/papers/p29-neumann-cidr20.pdf) for the
reasons.

The general idea is to store edges on disk by using an LSM like data structure.
Storing edge properties will be tricky because strict schema also has to be
introduced. Otherwise, it's impossible to store data on disk optimally (Neo4j
already has a pretty optimized implementation of that). Furthermore, we have to
introduce the paging concept.

This is a complex feature because various aspects of the core engine have to be
considered and probably updated (memory management, garbage collection,
indexing).

## References

* [On Disk IO, Part 3: LSM Trees](https://medium.com/databasss/on-disk-io-part-3-lsm-trees-8b2da218496f)
* [2020-04-13 On-disk Edge Store Research](https://docs.google.com/document/d/1avoR2g9dNWa4FSFt9NVn4JrT6uOAH_ReNeUoNVsJ7J4)
