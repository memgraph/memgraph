#pragma once

namespace utils {

// Inherit from this class to allow visiting from TVisitor class.
// Example usage:
//  class Expression : public Visitable<ExpressionVisitor> {
//  };
//  class Identifier : public ExpressionVisitor {
//   public:
//    DEFVISITABLE(ExpressionVisitor) // Use default Accept implementation
//    ....
//  };
//  class Literal : public Expression {
//   public:
//     void Accept(ExpressionVisitor &visitor) override {
//       // Implement custom Accept.
//     }
//  };
template <class TVisitor>
class Visitable {
 public:
  virtual ~Visitable() = default;
  virtual void Accept(TVisitor&) = 0;

#define DEFVISITABLE(TVisitor) \
  void Accept(TVisitor &visitor) override \
  { visitor.Visit(*this); visitor.PostVisit(*this); }

};

}
