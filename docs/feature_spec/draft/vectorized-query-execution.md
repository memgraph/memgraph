# Vectorized Query Execution

Memgraph query engine pulls one by one record during query execution. A more
efficient way would be to pull multiple records in an array. Adding that
shouldn't be complicated, but it wouldn't be advantageous without vectorizing
fetching records from the storage.

On the query engine level, the array could be part of the frame. In other
words, the frame and the code dealing with the frame has to be changed.
