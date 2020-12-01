# C++ Code Conventions

This chapter describes code conventions which should be followed when writing
C++ code.

## Code Style

Memgraph uses the
[Google Style Guide for C++](https://google.github.io/styleguide/cppguide.html)
in most of its code.  You should follow them whenever writing new code.
Besides following the style guide, take a look at
[Code Review Guidelines](code-review.md) for common design issues and pitfalls
with C++ as well as [Required Reading](required-reading.md).

### Often Overlooked Style Conventions

#### Pointers & References

References provide a shorter syntax for accessing members and better declare
the intent that a pointer *should* not be `nullptr`. They do not prevent
accessing a `nullptr` and obfuscate the client/calling code because the
reference argument is passed just like a value. Errors with such code have
been very difficult to debug. Therefore, pointers are always used. They will
not prevent bugs but will make some of them more obvious when reading code.

The only time a reference can be used is if it is `const`. Note that this
kind of reference is not allowed if it is stored somewhere, i.e. in a class.
You should use a pointer to `const` then. The primary reason being is that
references obscure the semantics of moving an object, thus making bugs with
references pointing to invalid memory harder to track down.

Example of this can be seen while capturing member variables by reference 
inside a lambda.
Let's define a class that has two members, where one of those members is a
lambda that captures the other member by reference.

```cpp
struct S {
  std::function<void()> foo;
  int bar;

  S() : foo([&]() { std::cout << bar; })
  {} 
};
```
What would happend if we move an instance of this object? Our lambda
reference capture will point to the same location as before, i.e. it 
will point to the **old** memory location of `bar`. This means we have
a dangling reference in our code!
There are multiple ways to avoid this. The simple solutions would be
capturing by value or disabling move constructors/assignments.
Still, if we capture by reference an object that is not a member
of the struct containing the lambda, we can still have a dangling
reference if we move that object somewhere in our code and there is
nothing we can do to prevent that.
So, be careful with lambda catptures, and remember that references are 
still a pointer under the hood!

[Style guide reference](https://google.github.io/styleguide/cppguide.html#Reference_Arguments)

#### Constructors & RAII

RAII (Resource Acquisition is Initialization) is a nice mechanism for managing
resources. It is especially useful when exceptions are used, such as in our
code. Unfortunately, they do have 2 major downsides.

  * Only exceptions can be used for to signal failure.
  * Calls to virtual methods are not resolved as expected.

For those reasons the style guide recommends minimal work that cannot fail.
Using virtual methods or doing a lot more should be delegated to some form of
`Init` method, possibly coupled with static factory methods. Similar rules
apply to destructors, which are not allowed to even throw exceptions.

[Style guide reference](https://google.github.io/styleguide/cppguide.html#Doing_Work_in_Constructors)

#### Constructors and member variables

One of the most powerful tools in C++ are the move semantics. We won't go into
detail how they work, but you should know how to utilize them as much as possible.
For our example we will define a small `struct` called `S` which contains only a single
member, `text` of type `std::string`.
```cpp
struct S {
  std::string text;
};
```
We want to define a constructor that receives a `std::string`, and saves its value in
`text`. You'll notice this situtation, in which you want to accept a value in constructor
and save it in object, often.

Our first implementation would look like this:
```cpp
S(const std::string &s) : text(s) {}
```

This is a valid solution but with one downside - we always copy.
If we construct an object like this:
```cpp
S s("some text");
```
We would create a temporary `std::string` object and then copy it to our member variable.

Of course, we know what to do now, we will capture temporary variables using `&&` and move it
into our `text` variable.
```cpp
S(std::string &&s) : text(std::move(s)) {}
```

Now let's add an extra member variable of type `std::vector<int>` called `words`.
Our constructors accept 2 values now - `std::vector<int>` and `std::string` and to
handle every possible solution optimally, we need to define a constructor for each
possible situation. We can send a temporary `std::vector` and a reference to 
a `std::string`, or we can send both values as temporaries. We have a lot of 
combinations for only 2 members.
Fortunately, there are 2 options, first one is writing a templated constructor:
```cpp
template<typename T1, typename T2>
S(T1 &&s, T2 &&v) : text(std::forward<T1>(s), words(std::forward<T2>(v) {}
```
But don't forget to define `requires` clause so you don't accept any type.
This solution is optimal but really hard to read AND write.
The second solution is something you should ALWAYS prefer in these simple cases:
```cpp
S(std::string s, std::vector<int> v) : text(std::move(s)), words(std::move(v)) {}
```
This way we have an almost optimal solution. The only extra operation we have is
the extra move when we send an `lvalue`. We would copy the value to the `s`, and then
move it to the `text` variable. Before, we would copy directly to `text`.
Also, you should ALWAYS write const-correct code, meaning `s` and `v` cannot be `const`
as it's not correct here. Why is that? You CANNOT move a const object! It would just degrade
to copying the object.
I would say that this is a small price to pay for a much cleaner and more maintainable code.

### Additional Style Conventions

Old code may have broken Google C++ Style accidentally, but the new code
should adhere to it as close as possible. We do have some exceptions
to Google style as well as additions for unspecified conventions.

#### Using C++ Exceptions

Unlike Google, we do not forbid using exceptions.

But, you should be very careful when using them and introducing new ones. They
are indeed handy, but cause problems with understanding the control flow since
exceptions are another form of `goto`. It also becomes very hard to determine
that the program is in correct state after the stack is unwound and the thrown
exception handled. Other than those issues, throwing exceptions in destructors
will terminate the program. The same will happen if a thread doesn't handle an
exception even though it is not the main thread.

[Style guide reference](https://google.github.io/styleguide/cppguide.html#Exceptions)

In general, when introducing a new exception, either via `throw` statement or
calling a function which throws, you must examine all transitive callers and
update their implementation and/or documentation.

#### Assertions

We use `CHECK` and `DCHECK` macros from glog library. You are encouraged to
use them as often as possible to both document and validate various pre and
post conditions of a function.

`CHECK` remains even in release build and should be preferred over it's cousin
`DCHECK` which only exists in debug builds. The primary reason is that you
want to trigger assertions in release builds in case the tests didn't
completely validate all code paths. It is better to fail fast and crash the
program, than to leave it in undefined state and potentially corrupt end
user's work. In cases when profiling shows that `CHECK` is causing visible
slowdown you should switch to `DCHECK`.

#### Template Parameter Naming

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
/// One sentence, brief description.
///
/// Long form description.
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

