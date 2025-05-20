These tests are to check that memgraph can accept various queries, regardless of validity.

Memgraph should not:
- crash
- have asan/ubsan issues
- valid queries should respond with rows
- invalid queries should respond with error
