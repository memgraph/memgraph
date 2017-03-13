#pragma once

#include <vector>

#include "utils/assert.hpp"
#include "query/backend/cpp/typed_value.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/typecheck/symbol_table.hpp"

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

  void PostVisit(NamedExpression &named_expression) override {
    auto &symbol = symbol_table_[named_expression];
    debug_assert(!result_stack_.empty(),
                 "The result of evaluating a named expression is missing.");
    frame_[symbol.position_] = result_stack_.back();
  }

  void Visit(Identifier &ident) override {
    result_stack_.push_back(frame_[symbol_table_[ident].position_]);
  }

 private:
  Frame &frame_;
  SymbolTable &symbol_table_;
  std::list<TypedValue> result_stack_;
};

}
