/// @file visitable.hpp

#pragma once

namespace utils {

/// Inherit from this class to allow visiting from TVisitor class.
/// Example usage:
///
///     class Expression : public Visitable<ExpressionVisitor> {
///     };
///     class Identifier : public ExpressionVisitor {
///      public:
///       DEFVISITABLE(ExpressionVisitor) // Use default Accept implementation
///       ....
///     };
///     class Literal : public Expression {
///      public:
///        void Accept(ExpressionVisitor &visitor) override {
///          // Implement custom Accept.
///          if (visitor.PreVisit(*this)) {
///            visitor.Visit(*this);
///            ...  // e.g. send visitor to children
///            visitor.PostVisit(*this);
///          }
///        }
///     };
///
/// @sa DEFVISITABLE
/// @sa Visitor
template <class TVisitor>
class Visitable {
 public:
  virtual ~Visitable() = default;
  virtual void Accept(TVisitor &) = 0;

/// Default implementation for @c Accept, which works for visitors of
/// @c TVisitor type.
/// @sa utils::Visitable
#define DEFVISITABLE(TVisitor)              \
  void Accept(TVisitor &visitor) override { \
    if (visitor.PreVisit(*this)) {          \
      visitor.Visit(*this);                 \
      visitor.PostVisit(*this);             \
    }                                       \
  }
};

}  // namespace utils
