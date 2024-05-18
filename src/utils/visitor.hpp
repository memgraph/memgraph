// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file visitor.hpp
///
/// @brief This file contains the generic implementation of visitor pattern.
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

#pragma once

namespace memgraph::utils {

// Don't use anonymous namespace, because each translation unit will then get a
// unique type. This may cause errors if one wants to check the type.
namespace detail {

template <typename R, class... T>
class VisitorBase;

template <typename R, class Head, class... Tail>
class VisitorBase<R, Head, Tail...> : public VisitorBase<R, Tail...> {
 public:
  using typename VisitorBase<R, Tail...>::ReturnType;
  using VisitorBase<R, Tail...>::Visit;
  virtual ReturnType Visit(Head &) = 0;
};

template <typename R, class T>
class VisitorBase<R, T> {
 public:
  /// @brief ReturnType of the @c Visit method.
  using ReturnType = R;
  virtual ~VisitorBase() = default;

  /// @brief Visit an instance of @c T.
  virtual ReturnType Visit(T &) = 0;
};

template <class... T>
class CompositeVisitorBase;

template <class Head, class... Tail>
class CompositeVisitorBase<Head, Tail...> : public CompositeVisitorBase<Tail...> {
 public:
  virtual bool PreVisit(Head &) { return DefaultPreVisit(); }
  virtual bool PostVisit(Head &) { return DefaultPostVisit(); }

  using CompositeVisitorBase<Tail...>::PreVisit;
  using CompositeVisitorBase<Tail...>::PostVisit;

 protected:
  using CompositeVisitorBase<Tail...>::DefaultPreVisit;
  using CompositeVisitorBase<Tail...>::DefaultPostVisit;
};

template <class T>
class CompositeVisitorBase<T> {
 public:
  /// @brief Start visiting an instance of *composite* type @c TVisitable.
  ///
  /// This function should be used to control whether the visitor should be sent
  /// further down the tree of classes. It is only called at the start of
  /// @c Accept method of a composite type. The default implementation returns
  /// true, which means that the visiting should continue.
  ///
  /// @return bool indicating whether to continue visiting.
  virtual bool PreVisit(T &) { return DefaultPreVisit(); }
  /// @brief Finish visiting an instance of *composite* type @c TVisitable.
  ///
  /// This function should be used to control whether the visitor should be sent
  /// to the siblings of currently visited instance. It is called at the end of
  /// @c Accept method in a composite type. The default implementation returns
  /// true, which means that visiting should continue.
  ///
  /// @return bool indicating whether to continue visiting.
  virtual bool PostVisit(T &) { return DefaultPostVisit(); }

  virtual ~CompositeVisitorBase() = default;

 protected:
  virtual bool DefaultPreVisit() { return true; }
  virtual bool DefaultPostVisit() { return true; }
};

}  // namespace detail

/// @brief Inherit from this class if you want to visit TVisitable types.
///
/// This visitor is the standard implementation of visitor pattern, where the
/// traversal should be done in the visitor implementation itself. This is
/// different from @c CompositeVisitor, where the traversal is handled by
/// visited classes. Therefore, this visitor contains only the @c Visit method,
/// which has a generic @c ReturnType.
///
/// Example usage:
/// @code
/// // Typedef for convenience or to establish a base class of visitors.
/// typedef Visitor<TypedValue, Identifier, AddOp> ExpressionVisitorBase;
/// class ExpressionVisitor : public ExpressionVisitorBase {
///  public:
///   using ExpressionVisitorBase::Visit;
///
///   TypedValue Visit(Identifier &ident) override {
///     // Visiting Identifier returns the value of it from execution frame.
///     return frame_[ident];
///   }
///   TypedValue Visit(AddOp &add_op) override {
///     // Visiting '+' sums the evaluation of both sides.
///     auto res1 = add_op.expression1_->Accept(*this);
///     auto res2 = add_op.expression2_->Accept(*this);
///     return res1 + res2;
///   }
/// };
/// @endcode
///
/// @sa Visitable
/// @sa CompositeVisitor
/// @sa detail::VisitorBase<R, T>::Visit
template <typename TReturn, class... TVisitable>
class Visitor : public detail::VisitorBase<TReturn, TVisitable...> {
 public:
  using typename detail::VisitorBase<TReturn, TVisitable...>::ReturnType;
  using detail::VisitorBase<TReturn, TVisitable...>::Visit;
};

/// @brief Inherit from this class if you want to visit *leaf* TVisitable types.
///
/// This visitor is meant for hierarchical visiting of classes, where the
/// traversal is done by the visited classes themselves. It should be paired
/// with @c CompositeVisitor.
///
/// The @c Visit method should return true, if the visitor wishes to continue
/// traversing the sibling leaf classes.
template <class... TVisitable>
using LeafVisitor = Visitor<bool, TVisitable...>;

/// @brief Inherit from this class if you want to visit *composite* TVisitable
/// types.
///
/// This visitor is meant for hierarchical visiting of classes, where the
/// traversal is done by the visited classes themselves. Therefore, this visitor
/// contains @c PreVisit and @c PostVisit methods which are only called when
/// entering and leaving *composite* classes. It should be paired with @c
/// LeafVisitor. The standard @c Visit method is called only on *leaf* classes,
/// which do not have any visitable children in them. If you wish to use the
/// regular visitor pattern, refer to @c Visitor.
///
/// Example usage:
/// @code
///
/// class ExpressionVisitor
///   : public CompositeVisitor<AddOp>,  // AddOp is a composite type
///     public LeafVisitor<Identifier> { // Identifier is a leaf type
///  public:
///   using CompositeVisitor::PreVisit;
///   using CompositeVisitor::PostVisit;
///   using LeafVisitor::Visit;
///
///   bool PreVisit(AddOp &add_op) override {
///     // Custom implementation for *composite* AddOp expression.
///   }
///
///   bool Visit(Identifier &identifier) override {
///     // Custom implementation for *leaf* Identifier expression.
///   }
/// };
/// @endcode
///
/// @sa Visitable
/// @sa LeafVisitor
/// @sa Visitor
/// @sa detail::CompositeVisitorBase<T>::PreVisit
/// @sa detail::CompositeVisitorBase<T>::PostVisit
template <class... TVisitable>
class CompositeVisitor : public detail::CompositeVisitorBase<TVisitable...> {
 public:
  using detail::CompositeVisitorBase<TVisitable...>::PreVisit;
  using detail::CompositeVisitorBase<TVisitable...>::PostVisit;

 protected:
  using detail::CompositeVisitorBase<TVisitable...>::DefaultPreVisit;
  using detail::CompositeVisitorBase<TVisitable...>::DefaultPostVisit;
};

/// @brief Inherit from this class to allow visiting from TVisitor class.
///
/// Example usage with @c CompositeVisitor:
/// @code
/// class Expression : public Visitable<ExpressionVisitor> { ... };
///
/// class Identifier : public Expression {
///  public:
///   // Use default Accept implementation, since this is a *leaf* type.
///   DEFVISITABLE(ExpressionVisitor)
///   ....
/// };
///
/// class AddOp : public Expression {
///  public:
///   // Implement custom Accept, since this is a *composite* type.
///   bool Accept(ExpressionVisitor &visitor) override {
///     if (visitor.PreVisit(*this)) {
///       // Send visitor to children. Accept returns bool, which when false
///       // should stop the traversal to siblings.
///       expression1_->Accept(*this) && expression2_->Accept(*this);
///     }
///     return visitor.PostVisit(*this);
///   }
///   ...
///
///  private:
///   Expression *expression1_;
///   Expression *expression1_;
///   ...
/// };
/// @endcode
///
/// @sa DEFVISITABLE
/// @sa Visitor
/// @sa LeafVisitor
/// @sa CompositeVisitor
template <class TVisitor>
class Visitable {
 public:
  virtual ~Visitable() = default;
  /// @brief Accept the @c TVisitor instance and call its @c Visit method.
  virtual typename TVisitor::ReturnType Accept(TVisitor &) = 0;

/// Default implementation for @c utils::Visitable::Accept, which works for
/// visitors of @c TVisitor type. This should be used to implement regular
/// @c utils::Visitor, as well as for *leaf* types when accepting
/// @c utils::CompositeVisitor.
///
/// @sa utils::Visitable
#define DEFVISITABLE(TVisitor) \
  TVisitor::ReturnType Accept(TVisitor &visitor) override { return visitor.Visit(*this); }
};

}  // namespace memgraph::utils
