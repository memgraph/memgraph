#pragma once

namespace utils {

// Don't use anonymous namespace, because each translation unit will then get a
// unique type. This may cause errors if one wants to check the type.
namespace detail {

template <typename T>
class VisitorBase {
 public:
  virtual ~VisitorBase() = default;

  virtual void Visit(T &) {}
  virtual void PostVisit(T &) {}
};

template <typename... T>
class RecursiveVisitorBase;

template <typename Head, typename... Tail>
class RecursiveVisitorBase<Head, Tail...>
    : public VisitorBase<Head>, public RecursiveVisitorBase<Tail...> {
 public:
  using VisitorBase<Head>::Visit;
  using VisitorBase<Head>::PostVisit;

  using RecursiveVisitorBase<Tail...>::Visit;
  using RecursiveVisitorBase<Tail...>::PostVisit;
};

template <typename T>
class RecursiveVisitorBase<T> : public VisitorBase<T> {
 public:
  using VisitorBase<T>::Visit;
  using VisitorBase<T>::PostVisit;
};

}  // namespace detail

/// Inherit from this class if you want to visit TVisitable types.
/// Example usage:
///
///     // Typedef for convenience or to establish a base class of visitors.
///     typedef Visitor<Identifier, Literal> ExpressionVisitorBase;
///     class ExpressionVisitor : public ExpressionVisitorBase {
///      public:
///       using ExpressionVisitorBase::Visit;
///       using ExpressionVisitorBase::PostVisit;
///
///       void Visit(Identifier &identifier) override {
///         // Custom implementation of visiting Identifier.
///       }
///     };
///
/// @sa Visitable
template <typename... TVisitable>
class Visitor : public detail::RecursiveVisitorBase<TVisitable...> {
 public:
  using detail::RecursiveVisitorBase<TVisitable...>::Visit;
  using detail::RecursiveVisitorBase<TVisitable...>::PostVisit;
};

}  // namespace utils
