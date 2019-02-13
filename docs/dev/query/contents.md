# Query Parsing, Planning and Execution

This part of the documentation deals with query execution.

Memgraph currently supports only query interpretation. Each new query is
parsed, analysed and translated into a sequence of operations which are then
executed on the main database storage. Query execution is organized into the
following phases:

  1.  [Lexical Analysis (Tokenization)](parsing.md)
  2.  [Syntactic Analysis (Parsing)](parsing.md)
  3.  [Semantic Analysis and Symbol Generation](semantic.md)
  4.  [Logical Planning](planning.md)
  5.  [Logical Plan Execution](execution.md)
