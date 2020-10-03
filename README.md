# Memgraph

Memgraph is an ACID compliant high performance transactional distributed
in-memory graph database featuring runtime native query compiling, lock free
data structures, multi-version concurrency control and asynchronous IO.

## Development Documentation

* [Quick Start](docs/dev/quick-start.md)
* [Workflow](docs/dev/workflow.md)
* [Storage](docs/dev/storage/contents.md)
* [Query Engine](docs/dev/query/contents.md)
* [Lisp C++ Preprocessor (LCP)](docs/dev/lcp.md)

## Feature Specifications

Each prominent Memgraph feature requires a feature specification. The purpose
of the feature specification is to have a base for discussing all aspects of
the feature. Elements of feature specifications should be:

* High-level context.
* Interface.
* User stories. Usage from the end-user perspective. In the case of a library,
  that should be cases on how to use the programming interface. In the case of
a shell script, that should be cases on how to use flags.
* Discussion about concurrency, memory management, error management.
* Any other essential functional or non-functional requirements.
* Test and benchmark strategy.
* Possible future changes/improvements/extensions.
* Security concerns.
* Additional and/or optional implementation details.

It's crucial to keep feature spec up-to-date with the implementation. Take a
look at the list of [feature specifications](docs/feature_spec/contents.md) to
learn more about powerful Memgraph features.

## User Documentation

Memgraph user documentation is maintained within
[docs](https://github.com/memgraph/docs) repository. The documentation is also
available on [GitBook](https://docs.memgraph.com).
