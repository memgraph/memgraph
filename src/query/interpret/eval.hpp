#pragma once

#include <map>
#include <vector>
#include <algorithm>

#include "database/graph_db_accessor.hpp"
#include "query/common.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/typed_value.hpp"
#include "utils/assert.hpp"
#include "utils/exceptions.hpp"

namespace query {

class ExpressionEvaluator : public TreeVisitorBase {
 public:
  ExpressionEvaluator(Frame &frame, const SymbolTable &symbol_table,
                      GraphDbAccessor &db_accessor,
                      GraphView graph_view = GraphView::AS_IS)
      : frame_(frame),
        symbol_table_(symbol_table),
        db_accessor_(db_accessor),
        graph_view_(graph_view) {}

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

  using TreeVisitorBase::PreVisit;
  using TreeVisitorBase::Visit;
  using TreeVisitorBase::PostVisit;

  void PostVisit(NamedExpression &named_expression) override {
    auto symbol = symbol_table_.at(named_expression);
    frame_[symbol] = PopBack();
  }

  void Visit(Identifier &ident) override {
    auto value = frame_[symbol_table_.at(ident)];
    SwitchAccessors(value);
    result_stack_.emplace_back(std::move(value));
  }

#define BINARY_OPERATOR_VISITOR(OP_NODE, CPP_OP)             \
  void PostVisit(OP_NODE &) override {                       \
    auto expression2 = PopBack();                            \
    auto expression1 = PopBack();                            \
    result_stack_.push_back(expression1 CPP_OP expression2); \
  }

#define UNARY_OPERATOR_VISITOR(OP_NODE, CPP_OP) \
  void PostVisit(OP_NODE &) override {          \
    auto expression = PopBack();                \
    result_stack_.push_back(CPP_OP expression); \
  }

  BINARY_OPERATOR_VISITOR(OrOperator, ||);
  BINARY_OPERATOR_VISITOR(XorOperator, ^);
  BINARY_OPERATOR_VISITOR(AndOperator, &&);
  BINARY_OPERATOR_VISITOR(AdditionOperator, +);
  BINARY_OPERATOR_VISITOR(SubtractionOperator, -);
  BINARY_OPERATOR_VISITOR(MultiplicationOperator, *);
  BINARY_OPERATOR_VISITOR(DivisionOperator, /);
  BINARY_OPERATOR_VISITOR(ModOperator, %);
  BINARY_OPERATOR_VISITOR(NotEqualOperator, !=);
  BINARY_OPERATOR_VISITOR(EqualOperator, ==);
  BINARY_OPERATOR_VISITOR(LessOperator, <);
  BINARY_OPERATOR_VISITOR(GreaterOperator, >);
  BINARY_OPERATOR_VISITOR(LessEqualOperator, <=);
  BINARY_OPERATOR_VISITOR(GreaterEqualOperator, >=);

  UNARY_OPERATOR_VISITOR(NotOperator, !);
  UNARY_OPERATOR_VISITOR(UnaryPlusOperator, +);
  UNARY_OPERATOR_VISITOR(UnaryMinusOperator, -);

  void PostVisit(IsNullOperator &) override {
    auto expression = PopBack();
    result_stack_.push_back(TypedValue(expression.IsNull()));
  }

#undef BINARY_OPERATOR_VISITOR
#undef UNARY_OPERATOR_VISITOR

  void PostVisit(PropertyLookup &property_lookup) override {
    auto expression_result = PopBack();
    switch (expression_result.type()) {
      case TypedValue::Type::Null:
        result_stack_.emplace_back(TypedValue::Null);
        break;
      case TypedValue::Type::Vertex:
        result_stack_.emplace_back(
            expression_result.Value<VertexAccessor>().PropsAt(
                property_lookup.property_));
        break;
      case TypedValue::Type::Edge: {
        result_stack_.emplace_back(
            expression_result.Value<EdgeAccessor>().PropsAt(
                property_lookup.property_));
        break;
      }
      case TypedValue::Type::Map:
        // TODO implement me
        throw utils::NotYetImplemented();
        break;

      default:
        throw TypedValueException(
            "Expected Node, Edge or Map for property lookup");
    }
  }

  void Visit(PrimitiveLiteral &literal) override {
    // TODO: no need to evaluate constants, we can write it to frame in one
    // of the previous phases.
    result_stack_.push_back(literal.value_);
  }

  void PostVisit(ListLiteral &literal) override {
    std::vector<TypedValue> result;
    result.reserve(literal.elements_.size());
    for (size_t i = 0 ; i < literal.elements_.size() ; i++)
      result.emplace_back(PopBack());
    std::reverse(result.begin(), result.end());
    result_stack_.emplace_back(std::move(result));
  }

  bool PreVisit(Aggregation &aggregation) override {
    auto value = frame_[symbol_table_.at(aggregation)];
    // Aggregation is probably always simple type, but let's switch accessor
    // just to be sure.
    SwitchAccessors(value);
    result_stack_.emplace_back(std::move(value));
    // Prevent evaluation of expressions inside the aggregation.
    return false;
  }

  void PostVisit(Function &function) override {
    std::vector<TypedValue> arguments;
    for (int i = 0; i < static_cast<int>(function.arguments_.size()); ++i) {
      arguments.push_back(PopBack());
    }
    reverse(arguments.begin(), arguments.end());
    result_stack_.emplace_back(function.function_(arguments, db_accessor_));
  }

 private:
  // If the given TypedValue contains accessors, switch them to New or Old,
  // depending on use_new_ flag.
  void SwitchAccessors(TypedValue &value) {
    if (graph_view_ == GraphView::AS_IS) return;
    switch (value.type()) {
      case TypedValue::Type::Vertex: {
        auto &vertex = value.Value<VertexAccessor>();
        switch (graph_view_) {
          case GraphView::NEW:
            vertex.SwitchNew();
            break;
          case GraphView::OLD:
            vertex.SwitchOld();
            break;
          default:
            permanent_fail("Unhandled GraphView enum");
        }
        break;
      }
      case TypedValue::Type::Edge: {
        auto &edge = value.Value<EdgeAccessor>();
        switch (graph_view_) {
          case GraphView::NEW:
            edge.SwitchNew();
            break;
          case GraphView::OLD:
            edge.SwitchOld();
            break;
          default:
            permanent_fail("Unhandled GraphView enum");
        }
        break;
      }
      case TypedValue::Type::List: {
        auto &list = value.Value<std::vector<TypedValue>>();
        for (auto &list_value : list) SwitchAccessors(list_value);
        break;
      }
      case TypedValue::Type::Map: {
        auto &map = value.Value<std::map<std::string, TypedValue>>();
        for (auto &kv : map) SwitchAccessors(kv.second);
        break;
      }
      default:
        break;
    }
  }

  Frame &frame_;
  const SymbolTable &symbol_table_;
  GraphDbAccessor &db_accessor_;
  // which switching approach should be used when evaluating
  const GraphView graph_view_;
  std::list<TypedValue> result_stack_;
};
}
