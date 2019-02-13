# Lexical and Syntactic Analysis

## Antlr

We use Antlr for lexical and syntax analysis of Cypher queries. Antrl uses
grammar file `Cypher.g4` downloaded from http://www.opencypher.org to generate
the parser and the visitor for the Cypher parse tree. Even though the provided
grammar is not very pleasant to work with we decided not to do any drastic
changes to it so that our transition to newly published versions of
`Cypher.g4` would be easier. Nevertheless, we had to fix some bugs and add
features, so our version is not completely the same.

In addition to using `Cypher.g4`, we have `MemgraphCypher.g4`. This grammar
file defines Memgraph specific extensions to the original grammar. Most
notable example is the inclusion of syntax for handling authorization. At the
moment, some extensions are also found in `Cypher.g4`. For example, the syntax
for using a lambda function in relationship patterns. These extensions should
be moved out of `Cypher.g4`, so that it remains as close to the original
grammar as possible. Additionally, having `MemgraphCypher.g4` may not be
enough if we wish to split the functionality for community and enterprise
editions of Memgraph.

## Abstract Syntax Tree (AST)

Since Antlr generated visitor and the official openCypher grammar are not very
practical to use, we translate the Antlr's AST to our own AST. Currently there
are ~40 types of nodes in our AST. Their definitions can be found in
`src/query/frontend/ast/ast.lcp`.

Major groups of types can be found under the following base types.

  * `Expression` --- types corresponding to Cypher expressions.
  * `Clause` --- types corresponding to Cypher clauses.
  * `PatternAtom` --- node or edge related information.
  * `Query` --- different kinds of queries, allows extending the language with
    Memgraph specific query syntax.

Memory management of created AST nodes is done with `AstStorage`. Each type
must be created by invoking `AstStorage::Create` method. This way all of the
pointers to nodes and their children are raw pointers. The only owner of
allocated memory is the `AstStorage`. When the storage goes out of scope, the
pointers become invalid. It may be more natural to handle tree ownership via
`unique_ptr`, i.e. each node owns its children. But there are some benefits to
having a custom storage and allocation scheme.

The primary reason we opted for not using `unique_ptr` is the requirement of
Antlr's base visitor class that the resulting values must by copyable. The
result is wrapped in `antlr::Any` so that the derived visitor classes may
return any type they wish when visiting Antlr's AST. Unfortunately,
`antlr::Any` does not work with non-copyable types.

Another benefit of having `AstStorage` is that we can easily add a different
allocation scheme for AST nodes. The interface of node creation would not
change.

### AST Translation

The translation process is done via `CypherMainVisitor` class, which is
derived from Antlr generated visitor. Besides instancing our AST types, a
minimal number of syntactic checks are done on a query. These checks handle
the cases which were valid in original openCypher grammar, but may be invalid
when combined with other syntax elements.
