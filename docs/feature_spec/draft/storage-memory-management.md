# Storage Memory Management

If Memgraph uses too much memory, OS will kill it. There has to be an internal
mechanism to control memory usage.

Since C++17, polymorphic allocators are an excellent way to inject custom
memory management while having a modular code. Memgraph already uses PMR in the
query execution. Also, refer to [1] on how to start with PMR in the storage
context.

## Resources

[1] [PMR: Mistakes Were Made](https://www.youtube.com/watch?v=6BLlIj2QoT8)
