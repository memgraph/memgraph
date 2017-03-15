#pragma once

#include <vector>
#include <utils/exceptions/not_yet_implemented.hpp>

#include "query/backend/cpp/typed_value.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "utils/assert.hpp"

namespace query {

class Frame {
 public:
  Frame(int size) : size_(size), elems_(size_) {}

  auto &operator[](int pos) { return elems_[pos]; }
  const auto &operator[](int pos) const { return elems_[pos]; }

 private:
  int size_;
  std::vector<TypedValue> elems_;
};

class ExpressionEvaluator : public TreeVisitorBase {
 public:
  ExpressionEvaluator(Frame &frame, SymbolTable &symbol_table)
      : frame_(frame), symbol_table_(symbol_table) {}

  /**
   * Removes and returns the last value from the result stack.
   * Consumers of this function are PostVisit functions for
   * expressions that consume subexpressions, as well as top
   * level expression consumers.
   */
  auto PopBack() {
    debug_assert(result_stack_.size() > 0, "Result stack empty");
    auto last = result_stack_.back();
    result_stack_.pop_back();
    return last;
  }

  void PostVisit(NamedExpression &named_expression) override {
    auto symbol = symbol_table_[named_expression];
    frame_[symbol.position_] = PopBack();
  }

  void Visit(Identifier &ident) override {
    result_stack_.push_back(frame_[symbol_table_[ident].position_]);
  }

  void PostVisit(PropertyLookup &property_lookup) override {
    auto expression_result = PopBack();
    switch (expression_result.type()) {
      case TypedValue::Type::Vertex:
        result_stack_.emplace_back(
            expression_result.Value<VertexAccessor>().PropsAt(
                property_lookup.property_));
        break;
      case TypedValue::Type::Edge:
        result_stack_.emplace_back(
            expression_result.Value<EdgeAccessor>().PropsAt(
                property_lookup.property_));
        break;

      case TypedValue::Type::Map:
        // TODO implement me
        throw NotYetImplemented();
        break;

      default:
        throw TypedValueException(
            "Expected Node, Edge or Map for property lookup");
    }
  }

 private:
  Frame &frame_;
  SymbolTable &symbol_table_;
  std::list<TypedValue> result_stack_;
};
}
