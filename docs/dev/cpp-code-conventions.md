# C++ Code Conventions

This chapter describes code conventions which should be followed when writing
C++ code.

## Code Style

Memgraph uses the Google Style Guide for C++ in most of its code. You should
follow them whenever writing new code. The style guide can be found
[here](https://google.github.io/styleguide/cppguide.html).

### Additional Style Conventions

Code style conventions which are left undefined by Google are specified here.

#### Template parameter naming

Template parameter names should start with capital letter 'T' followed by a
short descriptive name. For example:

```cpp
template <typename TKey, typename TValue>
class KeyValueStore
```

## Code Formatting

You should install `clang-format` and run it on code you change or add. The
root of Memgraph's project contains the `.clang-format` file, which specifies
how formatting should behave. Running `clang-format -style=file` in the
project's root will read the file and behave as expected. For ease of use, you
should integrate formatting with your favourite editor.

The code formatting isn't enforced, because sometimes manual formatting may
produce better results. Though, running `clang-format` is strongly encouraged.

## Documentation

Besides following the comment guidelines from [Google Style
Guide](https://google.github.io/styleguide/cppguide.html#Comments), your
documentation of the public API should be
[Doxygen](https://github.com/doxygen/doxygen) compatible. For private parts of
the code or for comments accompanying the implementation, you are free to
break doxygen compatibility. In both cases, you should write your
documentation as full sentences, correctly written in English.

## Doxygen

To start a Doxygen compatible documentation string, you should open your
comment with either a JavaDoc style block comment (`/**`) or a line comment
containing 3 slashes (`///`). Take a look at the 2 examples below.

### Block Comment

```cpp
/**
 * One sentence, brief description.
 *
 * Long form description.
 */
```

### Line Comment

```cpp
///
/// One sentence, brief description.
///
/// Long form description.
///
```

If you only have a brief description, you may collapse the documentation into
a single line.

### Block Comment

```cpp
/** Brief description. */
```

### Line Comment

```cpp
/// Brief description.
```

Whichever style you choose, keep it consistent across the whole file.

Doxygen supports various commands in comments, such as `@file` and `@param`.
These help Doxygen to render specified things differently or to track them for
cross referencing. If you want to learn more, take a look at these two links:

  * http://www.stack.nl/~dimitri/doxygen/manual/docblocks.html
  * http://www.stack.nl/~dimitri/doxygen/manual/commands.html

## Examples

Below are a few examples of documentation from the codebase.

### Function

```cpp
/**
 * Removes whitespace characters from the start and from the end of a string.
 *
 * @param s String that is going to be trimmed.
 *
 * @return Trimmed string.
 */
inline std::string Trim(const std::string &s);
```

### Class

```cpp
/** Base class for logical operators.
 *
 *  Each operator describes an operation, which is to be performed on the
 *  database. Operators are iterated over using a @c Cursor. Various operators
 *  can serve as inputs to others and thus a sequence of operations is formed.
 */
class LogicalOperator
    : public ::utils::Visitable<HierarchicalLogicalOperatorVisitor> {
 public:
  /** Constructs a @c Cursor which is used to run this operator.
   *
   *  @param GraphDbAccessor Used to perform operations on the database.
   */
  virtual std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) const = 0;

  /** Return @c Symbol vector where the results will be stored.
   *
   *  Currently, outputs symbols are only generated in @c Produce operator.
   *  @c Skip, @c Limit and @c OrderBy propagate the symbols from @c Produce (if
   *  it exists as input operator). In the future, we may want this method to
   *  return the symbols that will be set in this operator.
   *
   *  @param SymbolTable used to find symbols for expressions.
   *  @return std::vector<Symbol> used for results.
   */
  virtual std::vector<Symbol> OutputSymbols(const SymbolTable &) const {
    return std::vector<Symbol>();
  }

  virtual ~LogicalOperator() {}
};
```

### File Header

```cpp
/// @file visitor.hpp
///
/// This file contains the generic implementation of visitor pattern.
///
/// There are 2 approaches to the pattern:
///
///   * classic visitor pattern using @c Accept and @c Visit methods, and
///   * hierarchical visitor which also uses @c PreVisit and @c PostVisit
///   methods.
///
/// Classic Visitor
/// ===============
///
/// Explanation on the classic visitor pattern can be found from many
/// sources, but here is the link to hopefully most easily accessible
/// information: https://en.wikipedia.org/wiki/Visitor_pattern
///
/// The idea behind the generic implementation of classic visitor pattern is to
/// allow returning any type via @c Accept and @c Visit methods. Traversing the
/// class hierarchy is relegated to the visitor classes. Therefore, visitor
/// should call @c Accept on children when visiting their parents. To implement
/// such a visitor refer to @c Visitor and @c Visitable classes.
///
/// Hierarchical Visitor
/// ====================
///
/// Unlike the classic visitor, the intent of this design is to allow the
/// visited structure itself to control the traversal. This way the internal
/// children structure of classes can remain private. On the other hand,
/// visitors may want to differentiate visiting composite types from leaf types.
/// Composite types are those which contain visitable children, unlike the leaf
/// nodes. Differentiation is accomplished by providing @c PreVisit and @c
/// PostVisit methods, which should be called inside @c Accept of composite
/// types. Regular @c Visit is only called inside @c Accept of leaf types.
/// To implement such a visitor refer to @c CompositeVisitor, @c LeafVisitor and
/// @c Visitable classes.
///
/// Implementation of hierarchical visiting is modelled after:
/// http://wiki.c2.com/?HierarchicalVisitorPattern
```

