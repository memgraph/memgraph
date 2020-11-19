# Rust Query Modules

Memgraph provides the query modules infrastructure. It's possible to write
query modules in
[C/C++](https://docs.memgraph.com/memgraph/reference-overview/query-modules/c-api)
and
[Python](https://docs.memgraph.com/memgraph/reference-overview/query-modules/python-api).
The problem with C/C++ is that it's very error-prone and time-consuming.
Python's problem is that it's slow and has a bunch of other limitations listed
in the [feature spec](active-python-query-modules.md).

On the other hand, Rust is fast and much less error-prone compared to C. It
should be possible to use [bindgen](https://github.com/rust-lang/rust-bindgen)
to generate bindings out of the current C API and write wrapper code for Rust
developers to enjoy.
