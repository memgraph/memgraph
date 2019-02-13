# Semantic Analysis and Symbol Generation

In this phase, various semantic and variable type checks are performed.
Additionally, we generate symbols which map AST nodes to stored values
computed from evaluated expressions.

## Symbol Generation

Implementation can be found in `query/frontend/semantic/symbol_generator.cpp`.

Symbols are generated for each AST node that represents data that needs to
have storage. Currently, these are:

  * `NamedExpression`
  * `CypherUnion`
  * `Identifier`
  * `Aggregation`

You may notice that the above AST nodes may not correspond to something named
by a user. For example, `Aggregation` can be a part of larger expression and
thus remain unnamed. The reason we still generate symbols is to have a uniform
behaviour when executing a query as well as allow for caching the results of
expression evaluation.

AST nodes do not actually store a `Symbol` instance, instead they have a
`int32_t` index identifying the symbol in the `SymbolTable` class. This is
done to minimize the size of AST types as well as allow easier sharing of same
symbols with multiple instances of AST nodes.

The storage for evaluated data is represented by the `Frame` class. Each
symbol determines a unique position in the frame. During interpretation,
evaluation of expressions which have a symbol will either read or store values
in the frame. For example, instance of an `Identifier` will use the symbol to
find and read the value from `Frame`. On the other hand, `NamedExpression`
will take the result of evaluating its own expression and store it in the
`Frame`.

When a symbol is created, context of creation is used to assign a type to that
symbol. This type is used for simple type checking operations. For example,
`MATCH (n)` will create a symbol for variable `n`. Since the `MATCH (n)`
represents finding a vertex in the graph, we can set `Symbol::Type::Vertex`
for that symbol. Later, for example in `MATCH ()-[n]-()` we see that variable
`n` is used as an edge. Since we already have a symbol for that variable, we
detect this type mismatch and raise a `SemanticException`.

Basic rule of symbol generation, is that variables inside `MATCH`, `CREATE`,
`MERGE`, `WITH ... AS` and `RETURN ... AS` clauses establish new symbols.

### Symbols in Patterns

Inside `MATCH`, symbols are created only if they didn't exist before. For
example, patterns in `MATCH (n {a: 5})--(m {b: 5}) RETURN n, m` will create 2
symbols: one for `n` and one for `m`. `RETURN` clause will, in turn, reference
those symbols. Symbols established in a part of pattern are immediately bound
and visible in later parts. For example, `MATCH (n)--(n)` will create a symbol
for variable `n` for 1st `(n)`. That symbol is referenced in 2nd `(n)`. Note
that the symbol is not bound inside 1st `(n)` itself. What this means is that,
for example, `MATCH (n {a: n.b})` should raise an error, because `n` is not
yet bound when encountering `n.b`. On the other hand,
`MATCH (n)--(n {a: n.b})` is fine.

The `CREATE` is similar to `MATCH`, but it *always* establishes symbols for
variables which create graph elements. What this means is that, for example
`MATCH (n) CREATE (n)` is not allowed. `CREATE` wants to create a new node,
for which we already have a symbol. In such a case, we need to throw an error
that the variable `n` is being redeclared. On the other hand `MATCH (n) CREATE
(n)-[r :r]->(n)` is fine, because `CREATE` will only create the edge `r`,
connecting the already existing node `n`. Remaining behaviour is the same as
in `MATCH`. This means that we can simplify `CREATE` to be like `MATCH` with 2
special cases.

  1. Are we creating a node, i.e. `CREATE (n)`? If yes, then the symbol for
     `n` must not have been created before. Otherwise, we reference the
     existing symbol.
  2. Are we creating an edge, i.e. we encounter a variable for an edge inside
     `CREATE`? If yes, then that variable must not reference a symbol.

The `MERGE` clause is treated the same as `CREATE` with regards to symbol
generation. The only difference is that we allow bidirectional edges in the
pattern. When creating such a pattern, the direction of the created edge is
arbitrarily determined.

### Symbols in WITH and RETURN

In addition to patterns, new symbols are established in the `WITH` clause.
This clause makes the new symbols visible *only* to the rest of the query.
For example, `MATCH (old) WITH old AS new RETURN new, old` should raise an
error that `old` is unbound inside `RETURN`.

There is a special case with symbol visibility in `WHERE` and `ORDER BY`. They
need to see both the old and the new symbols. Therefore `MATCH (old) RETURN
old AS new ORDER BY old.prop` needs to work. On the other hand, if we perform
aggregations inside `WITH` or `RETURN`, then the old symbols should not be
visible neither in `WHERE` nor in `ORDER BY`. Since the aggregation has to go
through all the results in order to generate the final value, it makes no
sense to store old symbols and their values. A query like `MATCH (old) WITH
SUM(old.prop) AS sum WHERE old.prop = 42 RETURN sum` needs to raise an error
that `old` is unbound inside `WHERE`.

For cases when `SKIP` and `LIMIT` appear, we disallow any identifiers from
appearing in their expressions. Basically, `SKIP` and `LIMIT` can only be
constant expressions[^1]. For example, `MATCH (old) RETURN old AS new SKIP
new.prop` needs to raise that variables are not allowed in `SKIP`. It makes no
sense to allow variables, since their values may vary on each iteration. On
the other hand, we could support variables to constant expressions, but for
simplicity we do not. For example, `MATCH (old) RETURN old, 2 AS limit_var
LIMIT limit_var` would still throw an error.

Finally, we generate symbols for names created in `RETURN` clause. These
symbols are used for the final results of a query.

NOTE: New symbols in `WITH` and `RETURN` should be unique. This means that
`WITH a AS same, b AS same` is not allowed, neither is a construct like
`RETURN 2, 2`

### Symbols in Functions which Establish New Scope

Symbols can also be created in some functions. These functions usually take an
expression, bind a single variable and run the expression inside the newly
established scope.

The `all` function takes a list, creates a variable for list element and runs
the predicate expression. For example:

    MATCH (n) RETURN n, all(n IN n.prop_list WHERE n < 42)

We create a new symbol for use inside `all`, this means that the `WHERE n <
42` uses the `n` which takes values from a `n.prop_list` elements. The
original `n` bound by `MATCH` is not visible inside the `all` function, but it
is visible outside. Therefore, the `RETURN n` and `n.prop_list` reference the
`n` from `MATCH`.

[^1]: Constant expressions are expressions for which the result can be
  computed at compile time.
