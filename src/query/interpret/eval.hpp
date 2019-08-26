/// @file
#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <regex>
#include <vector>

#include "database/graph_db_accessor.hpp"
#include "query/common.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/typed_value.hpp"
#include "utils/exceptions.hpp"

namespace query {

class ExpressionEvaluator : public ExpressionVisitor<TypedValue> {
 public:
  ExpressionEvaluator(Frame *frame, const SymbolTable &symbol_table,
                      const EvaluationContext &ctx,
                      database::GraphDbAccessor *dba, GraphView graph_view)
      : frame_(frame),
        symbol_table_(&symbol_table),
        ctx_(&ctx),
        dba_(dba),
        graph_view_(graph_view) {}

  using ExpressionVisitor<TypedValue>::Visit;

  utils::MemoryResource *GetMemoryResource() const { return ctx_->memory; }

  TypedValue Visit(NamedExpression &named_expression) override {
    const auto &symbol = symbol_table_->at(named_expression);
    auto value = named_expression.expression_->Accept(*this);
    frame_->at(symbol) = value;
    return value;
  }

  TypedValue Visit(Identifier &ident) override {
    TypedValue value(frame_->at(symbol_table_->at(ident)), ctx_->memory);
    SwitchAccessors(value);
    return value;
  }

#define BINARY_OPERATOR_VISITOR(OP_NODE, CPP_OP, CYPHER_OP)              \
  TypedValue Visit(OP_NODE &op) override {                               \
    auto val1 = op.expression1_->Accept(*this);                          \
    auto val2 = op.expression2_->Accept(*this);                          \
    try {                                                                \
      return val1 CPP_OP val2;                                           \
    } catch (const TypedValueException &) {                              \
      throw QueryRuntimeException("Invalid types: {} and {} for '{}'.",  \
                                  val1.type(), val2.type(), #CYPHER_OP); \
    }                                                                    \
  }

#define UNARY_OPERATOR_VISITOR(OP_NODE, CPP_OP, CYPHER_OP)                 \
  TypedValue Visit(OP_NODE &op) override {                                 \
    auto val = op.expression_->Accept(*this);                              \
    try {                                                                  \
      return CPP_OP val;                                                   \
    } catch (const TypedValueException &) {                                \
      throw QueryRuntimeException("Invalid type {} for '{}'.", val.type(), \
                                  #CYPHER_OP);                             \
    }                                                                      \
  }

  BINARY_OPERATOR_VISITOR(OrOperator, ||, OR);
  BINARY_OPERATOR_VISITOR(XorOperator, ^, XOR);
  BINARY_OPERATOR_VISITOR(AdditionOperator, +, +);
  BINARY_OPERATOR_VISITOR(SubtractionOperator, -, -);
  BINARY_OPERATOR_VISITOR(MultiplicationOperator, *, *);
  BINARY_OPERATOR_VISITOR(DivisionOperator, /, /);
  BINARY_OPERATOR_VISITOR(ModOperator, %, %);
  BINARY_OPERATOR_VISITOR(NotEqualOperator, !=, <>);
  BINARY_OPERATOR_VISITOR(EqualOperator, ==, =);
  BINARY_OPERATOR_VISITOR(LessOperator, <, <);
  BINARY_OPERATOR_VISITOR(GreaterOperator, >, >);
  BINARY_OPERATOR_VISITOR(LessEqualOperator, <=, <=);
  BINARY_OPERATOR_VISITOR(GreaterEqualOperator, >=, >=);

  UNARY_OPERATOR_VISITOR(NotOperator, !, NOT);
  UNARY_OPERATOR_VISITOR(UnaryPlusOperator, +, +);
  UNARY_OPERATOR_VISITOR(UnaryMinusOperator, -, -);

#undef BINARY_OPERATOR_VISITOR
#undef UNARY_OPERATOR_VISITOR

  TypedValue Visit(AndOperator &op) override {
    auto value1 = op.expression1_->Accept(*this);
    if (value1.IsBool() && !value1.ValueBool()) {
      // If first expression is false, don't evaluate the second one.
      return value1;
    }
    auto value2 = op.expression2_->Accept(*this);
    try {
      return value1 && value2;
    } catch (const TypedValueException &) {
      throw QueryRuntimeException("Invalid types: {} and {} for AND.",
                                  value1.type(), value2.type());
    }
  }

  TypedValue Visit(IfOperator &if_operator) override {
    auto condition = if_operator.condition_->Accept(*this);
    if (condition.IsNull()) {
      return if_operator.then_expression_->Accept(*this);
    }
    if (condition.type() != TypedValue::Type::Bool) {
      // At the moment IfOperator is used only in CASE construct.
      throw QueryRuntimeException("CASE expected boolean expression, got {}.",
                                  condition.type());
    }
    if (condition.ValueBool()) {
      return if_operator.then_expression_->Accept(*this);
    }
    return if_operator.else_expression_->Accept(*this);
  }

  TypedValue Visit(InListOperator &in_list) override {
    auto literal = in_list.expression1_->Accept(*this);
    auto _list = in_list.expression2_->Accept(*this);
    if (_list.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    // Exceptions have higher priority than returning nulls when list expression
    // is not null.
    if (_list.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("IN expected a list, got {}.", _list.type());
    }
    const auto &list = _list.ValueList();

    // If literal is NULL there is no need to try to compare it with every
    // element in the list since result of every comparison will be NULL. There
    // is one special case that we must test explicitly: if list is empty then
    // result is false since no comparison will be performed.
    if (list.empty()) return TypedValue(false, ctx_->memory);
    if (literal.IsNull()) return TypedValue(ctx_->memory);

    auto has_null = false;
    for (const auto &element : list) {
      auto result = literal == element;
      if (result.IsNull()) {
        has_null = true;
      } else if (result.ValueBool()) {
        return TypedValue(true, ctx_->memory);
      }
    }
    if (has_null) {
      return TypedValue(ctx_->memory);
    }
    return TypedValue(false, ctx_->memory);
  }

  TypedValue Visit(SubscriptOperator &list_indexing) override {
    auto lhs = list_indexing.expression1_->Accept(*this);
    auto index = list_indexing.expression2_->Accept(*this);
    if (!lhs.IsList() && !lhs.IsMap() && !lhs.IsVertex() && !lhs.IsEdge() &&
        !lhs.IsNull())
      throw QueryRuntimeException(
          "Expected a list, a map, a node or an edge to index with '[]', got "
          "{}.",
          lhs.type());
    if (lhs.IsNull() || index.IsNull()) return TypedValue(ctx_->memory);
    if (lhs.IsList()) {
      if (!index.IsInt())
        throw QueryRuntimeException(
            "Expected an integer as a list index, got {}.", index.type());
      auto index_int = index.ValueInt();
      // NOTE: Take non-const reference to list, so that we can move out the
      // indexed element as the result.
      auto &list = lhs.ValueList();
      if (index_int < 0) {
        index_int += static_cast<int64_t>(list.size());
      }
      if (index_int >= static_cast<int64_t>(list.size()) || index_int < 0)
        return TypedValue(ctx_->memory);
      // NOTE: Explicit move is needed, so that we return the move constructed
      // value and preserve the correct MemoryResource.
      return std::move(list[index_int]);
    }

    if (lhs.IsMap()) {
      if (!index.IsString())
        throw QueryRuntimeException("Expected a string as a map index, got {}.",
                                    index.type());
      // NOTE: Take non-const reference to map, so that we can move out the
      // looked-up element as the result.
      auto &map = lhs.ValueMap();
      auto found = map.find(index.ValueString());
      if (found == map.end()) return TypedValue(ctx_->memory);
      // NOTE: Explicit move is needed, so that we return the move constructed
      // value and preserve the correct MemoryResource.
      return std::move(found->second);
    }

    if (lhs.IsVertex()) {
      if (!index.IsString())
        throw QueryRuntimeException(
            "Expected a string as a property name, got {}.", index.type());
      return TypedValue(lhs.ValueVertex().PropsAt(
                            dba_->Property(std::string(index.ValueString()))),
                        ctx_->memory);
    }

    if (lhs.IsEdge()) {
      if (!index.IsString())
        throw QueryRuntimeException(
            "Expected a string as a property name, got {}.", index.type());
      return TypedValue(lhs.ValueEdge().PropsAt(
                            dba_->Property(std::string(index.ValueString()))),
                        ctx_->memory);
    }

    // lhs is Null
    return TypedValue(ctx_->memory);
  }

  TypedValue Visit(ListSlicingOperator &op) override {
    // If some type is null we can't return null, because throwing exception
    // on illegal type has higher priority.
    auto is_null = false;
    auto get_bound = [&](Expression *bound_expr, int64_t default_value) {
      if (bound_expr) {
        auto bound = bound_expr->Accept(*this);
        if (bound.type() == TypedValue::Type::Null) {
          is_null = true;
        } else if (bound.type() != TypedValue::Type::Int) {
          throw QueryRuntimeException(
              "Expected an integer for a bound in list slicing, got {}.",
              bound.type());
        }
        return bound;
      }
      return TypedValue(default_value, ctx_->memory);
    };
    auto _upper_bound =
        get_bound(op.upper_bound_, std::numeric_limits<int64_t>::max());
    auto _lower_bound = get_bound(op.lower_bound_, 0);

    auto _list = op.list_->Accept(*this);
    if (_list.type() == TypedValue::Type::Null) {
      is_null = true;
    } else if (_list.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("Expected a list to slice, got {}.",
                                  _list.type());
    }

    if (is_null) {
      return TypedValue(ctx_->memory);
    }
    const auto &list = _list.ValueList();
    auto normalise_bound = [&](int64_t bound) {
      if (bound < 0) {
        bound = static_cast<int64_t>(list.size()) + bound;
      }
      return std::max(static_cast<int64_t>(0),
                      std::min(bound, static_cast<int64_t>(list.size())));
    };
    auto lower_bound = normalise_bound(_lower_bound.ValueInt());
    auto upper_bound = normalise_bound(_upper_bound.ValueInt());
    if (upper_bound <= lower_bound) {
      return TypedValue(TypedValue::TVector(ctx_->memory), ctx_->memory);
    }
    return TypedValue(TypedValue::TVector(
        list.begin() + lower_bound, list.begin() + upper_bound, ctx_->memory));
  }

  TypedValue Visit(IsNullOperator &is_null) override {
    auto value = is_null.expression_->Accept(*this);
    return TypedValue(value.IsNull(), ctx_->memory);
  }

  TypedValue Visit(PropertyLookup &property_lookup) override {
    auto expression_result = property_lookup.expression_->Accept(*this);
    switch (expression_result.type()) {
      case TypedValue::Type::Null:
        return TypedValue(ctx_->memory);
      case TypedValue::Type::Vertex:
        return TypedValue(expression_result.ValueVertex().PropsAt(
                              GetProperty(property_lookup.property_)),
                          ctx_->memory);
      case TypedValue::Type::Edge:
        return TypedValue(expression_result.ValueEdge().PropsAt(
                              GetProperty(property_lookup.property_)),
                          ctx_->memory);
      case TypedValue::Type::Map: {
        // NOTE: Take non-const reference to map, so that we can move out the
        // looked-up element as the result.
        auto &map = expression_result.ValueMap();
        auto found = map.find(property_lookup.property_.name.c_str());
        if (found == map.end()) return TypedValue(ctx_->memory);
        // NOTE: Explicit move is needed, so that we return the move constructed
        // value and preserve the correct MemoryResource.
        return std::move(found->second);
      }
      default:
        throw QueryRuntimeException(
            "Only nodes, edges and maps have properties to be looked-up.");
    }
  }

  TypedValue Visit(LabelsTest &labels_test) override {
    auto expression_result = labels_test.expression_->Accept(*this);
    switch (expression_result.type()) {
      case TypedValue::Type::Null:
        return TypedValue(ctx_->memory);
      case TypedValue::Type::Vertex: {
        const auto &vertex = expression_result.ValueVertex();
        for (const auto &label : labels_test.labels_) {
          if (!vertex.has_label(GetLabel(label))) {
            return TypedValue(false, ctx_->memory);
          }
        }
        return TypedValue(true, ctx_->memory);
      }
      default:
        throw QueryRuntimeException("Only nodes have labels.");
    }
  }

  TypedValue Visit(PrimitiveLiteral &literal) override {
    // TODO: no need to evaluate constants, we can write it to frame in one
    // of the previous phases.
    return TypedValue(literal.value_, ctx_->memory);
  }

  TypedValue Visit(ListLiteral &literal) override {
    TypedValue::TVector result(ctx_->memory);
    result.reserve(literal.elements_.size());
    for (const auto &expression : literal.elements_)
      result.emplace_back(expression->Accept(*this));
    return TypedValue(result, ctx_->memory);
  }

  TypedValue Visit(MapLiteral &literal) override {
    TypedValue::TMap result(ctx_->memory);
    for (const auto &pair : literal.elements_)
      result.emplace(pair.first.name, pair.second->Accept(*this));
    return TypedValue(result, ctx_->memory);
  }

  TypedValue Visit(Aggregation &aggregation) override {
    TypedValue value(frame_->at(symbol_table_->at(aggregation)), ctx_->memory);
    // Aggregation is probably always simple type, but let's switch accessor
    // just to be sure.
    SwitchAccessors(value);
    return value;
  }

  TypedValue Visit(Coalesce &coalesce) override {
    auto &exprs = coalesce.expressions_;

    if (exprs.size() == 0) {
      throw QueryRuntimeException("'coalesce' requires at least one argument.");
    }

    for (int64_t i = 0; i < exprs.size(); ++i) {
      TypedValue val(exprs[i]->Accept(*this), ctx_->memory);
      if (!val.IsNull()) {
        return val;
      }
    }

    return TypedValue(ctx_->memory);
  }

  TypedValue Visit(Function &function) override {
    FunctionContext function_ctx{dba_, ctx_->memory, ctx_->timestamp,
                                 &ctx_->counters};
    // Stack allocate evaluated arguments when there's a small number of them.
    if (function.arguments_.size() <= 8) {
      TypedValue arguments[8] = {
          TypedValue(ctx_->memory), TypedValue(ctx_->memory),
          TypedValue(ctx_->memory), TypedValue(ctx_->memory),
          TypedValue(ctx_->memory), TypedValue(ctx_->memory),
          TypedValue(ctx_->memory), TypedValue(ctx_->memory)};
      for (size_t i = 0; i < function.arguments_.size(); ++i) {
        arguments[i] = function.arguments_[i]->Accept(*this);
      }
      auto res = function.function_(arguments, function.arguments_.size(),
                                    function_ctx);
      CHECK(res.GetMemoryResource() == ctx_->memory);
      return res;
    } else {
      TypedValue::TVector arguments(ctx_->memory);
      arguments.reserve(function.arguments_.size());
      for (const auto &argument : function.arguments_) {
        arguments.emplace_back(argument->Accept(*this));
      }
      auto res =
          function.function_(arguments.data(), arguments.size(), function_ctx);
      CHECK(res.GetMemoryResource() == ctx_->memory);
      return res;
    }
  }

  TypedValue Visit(Reduce &reduce) override {
    auto list_value = reduce.list_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("REDUCE expected a list, got {}.",
                                  list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &element_symbol = symbol_table_->at(*reduce.identifier_);
    const auto &accumulator_symbol = symbol_table_->at(*reduce.accumulator_);
    auto accumulator = reduce.initializer_->Accept(*this);
    for (const auto &element : list) {
      frame_->at(accumulator_symbol) = accumulator;
      frame_->at(element_symbol) = element;
      accumulator = reduce.expression_->Accept(*this);
    }
    return accumulator;
  }

  TypedValue Visit(Extract &extract) override {
    auto list_value = extract.list_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("EXTRACT expected a list, got {}.",
                                  list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &element_symbol = symbol_table_->at(*extract.identifier_);
    TypedValue::TVector result(ctx_->memory);
    result.reserve(list.size());
    for (const auto &element : list) {
      if (element.IsNull()) {
        result.emplace_back();
      } else {
        frame_->at(element_symbol) = element;
        result.emplace_back(extract.expression_->Accept(*this));
      }
    }
    return TypedValue(result, ctx_->memory);
  }

  TypedValue Visit(All &all) override {
    auto list_value = all.list_expression_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("ALL expected a list, got {}.",
                                  list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &symbol = symbol_table_->at(*all.identifier_);
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = all.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException(
            "Predicate of ALL must evaluate to boolean, got {}.",
            result.type());
      }
      if (result.IsNull() || !result.ValueBool()) {
        return result;
      }
    }
    return TypedValue(true, ctx_->memory);
  }

  TypedValue Visit(Single &single) override {
    auto list_value = single.list_expression_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("SINGLE expected a list, got {}.",
                                  list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &symbol = symbol_table_->at(*single.identifier_);
    bool predicate_satisfied = false;
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = single.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException(
            "Predicate of SINGLE must evaluate to boolean, got {}.",
            result.type());
      }
      if (result.IsNull() || !result.ValueBool()) {
        continue;
      }
      // Return false if more than one element satisfies the predicate.
      if (predicate_satisfied) {
        return TypedValue(false, ctx_->memory);
      } else {
        predicate_satisfied = true;
      }
    }
    return TypedValue(predicate_satisfied, ctx_->memory);
  }

  TypedValue Visit(ParameterLookup &param_lookup) override {
    return TypedValue(
        ctx_->parameters.AtTokenPosition(param_lookup.token_position_),
        ctx_->memory);
  }

  TypedValue Visit(RegexMatch &regex_match) override {
    auto target_string_value = regex_match.string_expr_->Accept(*this);
    auto regex_value = regex_match.regex_->Accept(*this);
    if (target_string_value.IsNull() || regex_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (regex_value.type() != TypedValue::Type::String) {
      throw QueryRuntimeException(
          "Regular expression must evaluate to a string, got {}.",
          regex_value.type());
    }
    if (target_string_value.type() != TypedValue::Type::String) {
      // Instead of error, we return Null which makes it compatible in case we
      // use indexed lookup which filters out any non-string properties.
      // Assuming a property lookup is the target_string_value.
      return TypedValue(ctx_->memory);
    }
    const auto &target_string = target_string_value.ValueString();
    try {
      std::regex regex(regex_value.ValueString());
      return TypedValue(std::regex_match(target_string, regex), ctx_->memory);
    } catch (const std::regex_error &e) {
      throw QueryRuntimeException("Regex error in '{}': {}",
                                  regex_value.ValueString(), e.what());
    }
  }

 private:
  storage::Property GetProperty(PropertyIx prop) {
    return ctx_->properties[prop.ix];
  }

  storage::Label GetLabel(LabelIx label) {
    return ctx_->labels[label.ix];
  }

  // If the given TypedValue contains accessors, switch them to New or Old,
  // depending on use_new_ flag.
  void SwitchAccessors(TypedValue &value) {
    switch (value.type()) {
      case TypedValue::Type::Vertex: {
        auto &vertex = value.ValueVertex();
        switch (graph_view_) {
          case GraphView::NEW:
            vertex.SwitchNew();
            break;
          case GraphView::OLD:
            vertex.SwitchOld();
            break;
          default:
            LOG(FATAL) << "Unhandled GraphView enum";
        }
        break;
      }
      case TypedValue::Type::Edge: {
        auto &edge = value.ValueEdge();
        switch (graph_view_) {
          case GraphView::NEW:
            edge.SwitchNew();
            break;
          case GraphView::OLD:
            edge.SwitchOld();
            break;
          default:
            LOG(FATAL) << "Unhandled GraphView enum";
        }
        break;
      }
      case TypedValue::Type::List: {
        auto &list = value.ValueList();
        for (auto &list_value : list) SwitchAccessors(list_value);
        break;
      }
      case TypedValue::Type::Map: {
        auto &map = value.ValueMap();
        for (auto &kv : map) SwitchAccessors(kv.second);
        break;
      }
      case TypedValue::Type::Path:
        switch (graph_view_) {
          case GraphView::NEW:
            value.ValuePath().SwitchNew();
            break;
          case GraphView::OLD:
            value.ValuePath().SwitchOld();
            break;
          default:
            LOG(FATAL) << "Unhandled GraphView enum";
        }
      case TypedValue::Type::Null:
      case TypedValue::Type::Bool:
      case TypedValue::Type::String:
      case TypedValue::Type::Int:
      case TypedValue::Type::Double:
        break;
    }
  }

  Frame *frame_;
  const SymbolTable *symbol_table_;
  const EvaluationContext *ctx_;
  database::GraphDbAccessor *dba_;
  // which switching approach should be used when evaluating
  const GraphView graph_view_;
};

/// A helper function for evaluating an expression that's an int.
///
/// @param what - Name of what's getting evaluated. Used for user feedback (via
///               exception) when the evaluated value is not an int.
/// @throw QueryRuntimeException if expression doesn't evaluate to an int.
inline int64_t EvaluateInt(ExpressionEvaluator *evaluator, Expression *expr,
                           const std::string &what) {
  TypedValue value = expr->Accept(*evaluator);
  try {
    return value.ValueInt();
  } catch (TypedValueException &e) {
    throw QueryRuntimeException(what + " must be an int");
  }
}

}  // namespace query
