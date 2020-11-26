# Query Compilation

Memgraph supports the interpretation of queries in a pull-based way. An
advantage of interpreting queries is a fast time until the execution, which is
convenient when a user wants to test a bunch of queries in a short time. The
downside is slow runtime. The runtime could be improved by compiling query
plans.

## Research Area 1

The easiest route to the query compilation might be generating [virtual
constexpr](http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p1064r0.html)
pull functions, making a dynamic library out of the entire compiled query plan,
and swapping query plans during the database runtime.
