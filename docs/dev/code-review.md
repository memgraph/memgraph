# Code Review Guidelines

This chapter describes some of the things you should be on the lookout when
reviewing someone else's code.

## Pointers & References

In cases when some code passes a pointer or reference, or if a code stores a
pointer or reference you should take a careful look at the following.

  * Lifetime of the pointed to value (this includes both ownership and
    multithreaded access).
  * In case of a class, check validity of destructor and move/copy
    constructors.
  * Is the pointed to value mutated, if not it should be `const` (`const Type
    *` or `const Type &`).

## Allocators & Memory Resources

With the introduction of polymorphic allocators (C++17 `<memory_resource>` and
our `utils/memory.hpp`) we get a more convenient type signatures for
containers so as to keep the outward facing API nice. This convenience comes
at a cost of less static checks on the type level due to type erasure.

For example:

    std::pmr::vector<int> first_vec(std::pmr::null_memory_resource());
    std::pmr::vector<int> second_vec(std::pmr::new_delete_resource());

    second_vec = first_vec  // What happens here?

    // Or with our implementation
    utils::MonotonicBufferResource monotonic_memory(1024);
    std::vector<int, utils::Allocator<int>> first_vec(&monotonic_memory);
    std::vector<int, utils::Allocator<int>> second_vec(utils::NewDeleteResource());

    second_vec = first_vec  // What happens here?

In the above, both `first_vec` and `second_vec` have the same type, but have
*different* allocators! This can lead to ambiguity when moving or copying
elements between them.

You need to watch out for the following.

  * Swapping can lead to undefined behaviour if the allocators are not equal.
  * Is the move construction done with the right allocator.
  * Is the move assignment done correctly, also it may throw an exception.
  * Is the copy construction done with the right allocator.
  * Is the copy assignment done correctly.

## Classes & Object Oriented Programming

A common mistake is to use classes, inheritance and "OOP" when it's not
needed. This sections shows examples of encountered cases.

### Classes without (Meaningful) Members

    class MyCoolClass {
     public:
      int BeCool(int a, int b) { return a + b; }

      void SaySomethingCool() { std::cout << "Hello!"; }
    };

The above class has no members (i.e. state) which affect the behaviour of
methods. This class should need not exist, it can be easily replaced with a
more modular (and shorter) design -- top level functions.

    int BeCool(int a, int b) { return a + b; }

    void SaySomethingCool() { std::cout << "Hello!"; }

### Classes with a Single Public Method

    clas MyAwesomeClass {
     public:
      MyAwesomeClass(int state) : state_(state) {}

      int GetAwesome() { return GetAwesomeImpl() + 1; }

     private:
      int state_;

      int GetAwesomeImpl() { return state_; }
    };

The above class has a `state_` and even a private method, but there's only one
public method -- `GetAwesome`.

You should check "Does the stored state have any meaningful influence on the
public method?", similarly to the previous point.

In the above case it doesn't, and the class should be replaced with a public
function in `.hpp` while the private method should become a private function
in `.cpp` (static or in anonymous namespace).

    // hpp
    int GetAwesome(int state);

    // cpp
    namespace {
      int GetAwesomeImpl(int state) { return state; }
    }
    int GetAwesome(int state) { return GetAwesomeImpl(state) + 1; }

A counterexample is when the state is meaningful.

    class Counter {
     public:
      Counter(int state) : state_(state) {}

      int Get() { return state_++; }

     private:
      int state_;
    };

But even that could be replaced with a closure.

    auto MakeCounter(int state) {
      return [state]() mutable { return state++; };
    }

### Private Methods

Instead of private methods, top level functions should be preferred. The
reasoning is completely explained in "Effective C++" Item 23 by Scott Meyers.
In our codebase, even improvements to compilation times can be noticed if
private methods in interface (`.hpp`) files are replaced with top level
functions in implementation (`.cpp`) files.

### Inheritance

The rule is simple -- if there are no virtual methods (but maybe destructor),
then the class should be marked as `final` and never inherited.

If there are virtual methods (i.e. class is meant to be inherited), make sure
that either a public virtual destructor or a protected non-virtual destructor
exist. See "Effective C++" Item 7 by Scott Meyers. Also take a look at
"Effective C++" Items 32---39 by Scott Meyers.

An example of how inheritance with no virtual methods is replaced with
composition.

    class MyBase {
     public:
      virtual ~MyBase() {}

      void DoSomethingBase() { ... }
    };

    class MyDerived final : public MyBase {
     public:
      void DoSomethingNew() { ... DoSomethingBase(); ... }
    };

With composition, the above becomes.

    class MyBase final {
     public:
      void DoSomethingBase() { ... }
    };

    class MyDerived final {
      MyBase base_;

     public:
      void DoSomethingNew() { ... base_.DoSomethingBase(); ... }
    };

The composition approach is preferred as it encapsulates the fact that
`MyBase` is used for the implementation and users only interact with the
public interface of `MyDerived`. Additionally, you can easily replace `MyBase
base_;` with a C++ PIMPL idiom (`std::unique_ptr<MyBase> base_;`) to make the
code more modular with regards to compilation.

More advanced C++ users will recognize that the encapsulation feature of the
non-PIMPL composition can be replaced with private inheritance.

    class MyDerived final : private MyBase {
     public:
      void DoSomethingNew() { ... MyBase::DoSomethingBase(); ... }
    };

One of the common "counterexample" is the ability to store objects of
different type in a container or pass them to a function. Unfortunately, this
is not that good of a design. For example.

    class MyBase {
      ... // No virtual methods (but the destructor)
    };

    class MyFirstClass final : public MyBase { ...  };

    class MySecondClass final : public MyBase { ... };

    std::vector<std::unique_ptr<MyBase>> first_and_second_classes;
    first_and_second_classes.push_back(std::make_unique<MyFirstClass>());
    first_and_second_classes.push_back(std::make_unique<MySecondClass>());

    void FunctionOnFirstOrSecond(const MyBase &first_or_second, ...) { ... }

With C++17, the containers for different types should be implemented with
`std::variant`, and as before the functions can be templated.

    class MyFirstClass final { ... };

    class MySecondClass final { ... };

    std::vector<std::variant<MyFirstClass, MySecondClass>> first_and_second_classes;
    // Notice no heap allocation, since we don't store a pointer
    first_and_second_classes.emplace_back(MyFirstClass());
    first_and_second_classes.emplace_back(MySecondClass());

    // You can also use `std::variant` here instead of template
    template <class TFirstOrSecond>
    void FunctionOnFirstOrSecond(const TFirstOrSecond &first_or_second, ...) { ... }

Naturally, if the base class has meaningful virtual methods (i.e. other than
destructor) it maybe is OK to use inheritance but also consider alternatives.
See "Effective C++" Items 32---39 by Scott Meyers.

### Multiple Inheritance

Multiple inheritance should not be used unless all base classes are pure
interface classes. This decision is inherited from [Google C++ Style
Guide](https://google.github.io/styleguide/cppguide.html#Inheritance). For
example on how to design with and around multiple inheritance refer to
"Effective C++" Item 40 by Scott Meyers.

Naturally, if there *really* is no better design, then multiple inheritance is
allowed. An example of this can be found in our codebase when inheriting
Visitor classes (though even that could be replaced with `std::variant` for
example).

## Code Format & Style

If something doesn't conform to our code formatting and style, just refer the
author to either [C++ Style](cpp-code-conventions.md) or [Other Code
Conventions](other-code-conventions.md).
