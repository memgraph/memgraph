// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v3/expr.hpp"

#include <vector>

#include "db_accessor.hpp"
#include "opencypher/parser.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/bindings/ast/ast.hpp"
#include "storage/v3/bindings/bindings.hpp"
#include "storage/v3/bindings/cypher_main_visitor.hpp"
#include "storage/v3/bindings/db_accessor.hpp"
#include "storage/v3/bindings/eval.hpp"
#include "storage/v3/bindings/frame.hpp"
#include "storage/v3/bindings/symbol_generator.hpp"
#include "storage/v3/bindings/symbol_table.hpp"
#include "storage/v3/bindings/typed_value.hpp"
#include "storage/v3/value_conversions.hpp"

namespace memgraph::storage::v3 {

msgs::Value ConstructValueVertex(const VertexAccessor &acc, View view) {
  // Get the vertex id
  auto prim_label = acc.PrimaryLabel(view).GetValue();
  memgraph::msgs::Label value_label{.id = prim_label};

  auto prim_key = conversions::ConvertValueVector(acc.PrimaryKey(view).GetValue());
  memgraph::msgs::VertexId vertex_id = std::make_pair(value_label, prim_key);

  // Get the labels
  auto vertex_labels = acc.Labels(view).GetValue();
  std::vector<memgraph::msgs::Label> value_labels;
  value_labels.reserve(vertex_labels.size());

  std::transform(vertex_labels.begin(), vertex_labels.end(), std::back_inserter(value_labels),
                 [](const auto &label) { return msgs::Label{.id = label}; });

  return msgs::Value({.id = vertex_id, .labels = value_labels});
}

msgs::Value ConstructValueEdge(const EdgeAccessor &acc, View view) {
  msgs::EdgeType type = {.id = acc.EdgeType().AsUint()};
  msgs::EdgeId gid = {.gid = acc.Gid().AsUint()};

  msgs::Label src_prim_label = {.id = acc.FromVertex().primary_label};
  memgraph::msgs::VertexId src_vertex =
      std::make_pair(src_prim_label, conversions::ConvertValueVector(acc.FromVertex().primary_key));

  msgs::Label dst_prim_label = {.id = acc.ToVertex().primary_label};
  msgs::VertexId dst_vertex =
      std::make_pair(dst_prim_label, conversions::ConvertValueVector(acc.ToVertex().primary_key));

  std::optional<std::vector<std::pair<PropertyId, msgs::Value>>> properties_opt;
  const auto &properties = acc.Properties(view);

  if (properties.HasValue()) {
    const auto &props = properties.GetValue();
    std::vector<std::pair<PropertyId, msgs::Value>> present_properties;
    present_properties.reserve(props.size());

    std::transform(props.begin(), props.end(), std::back_inserter(present_properties), [](const auto &prop) {
      return std::make_pair(prop.first, conversions::FromPropertyValueToValue(prop.second));
    });

    properties_opt = std::move(present_properties);
  }

  return msgs::Value({.src = src_vertex, .dst = dst_vertex, .properties = properties_opt, .id = gid, .type = type});
}

msgs::Value FromTypedValueToValue(TypedValue &&tv) {
  switch (tv.type()) {
    case TypedValue::Type::Bool:
      return msgs::Value(tv.ValueBool());
    case TypedValue::Type::Double:
      return msgs::Value(tv.ValueDouble());
    case TypedValue::Type::Int:
      return msgs::Value(tv.ValueInt());
    case TypedValue::Type::List: {
      std::vector<msgs::Value> list;
      auto &tv_list = tv.ValueList();
      list.reserve(tv_list.size());
      std::transform(tv_list.begin(), tv_list.end(), std::back_inserter(list),
                     [](auto &elem) { return FromTypedValueToValue(std::move(elem)); });
      return msgs::Value(list);
    }
    case TypedValue::Type::Map: {
      std::map<std::string, msgs::Value> map;
      for (auto &[key, val] : tv.ValueMap()) {
        map.emplace(key, FromTypedValueToValue(std::move(val)));
      }
      return msgs::Value(map);
    }
    case TypedValue::Type::Null:
      return {};
    case TypedValue::Type::String:
      return msgs::Value((std::string(tv.ValueString())));
    case TypedValue::Type::Vertex:
      return ConstructValueVertex(tv.ValueVertex(), View::OLD);
    case TypedValue::Type::Edge:
      return ConstructValueEdge(tv.ValueEdge(), View::OLD);

    // TBD -> we need to specify temporal types, not a priority.
    case TypedValue::Type::Date:
    case TypedValue::Type::LocalTime:
    case TypedValue::Type::LocalDateTime:
    case TypedValue::Type::Duration:
    case TypedValue::Type::Path: {
      MG_ASSERT(false, "This conversion between TypedValue and Value is not implemented yet!");
      break;
    }
  }
  return {};
}

std::vector<msgs::Value> ConvertToValueVectorFromTypedValueVector(
    std::vector<memgraph::storage::v3::TypedValue> &&vec) {
  std::vector<msgs::Value> ret;
  ret.reserve(vec.size());

  std::transform(vec.begin(), vec.end(), std::back_inserter(ret),
                 [](auto &elem) { return FromTypedValueToValue(std::move(elem)); });
  return ret;
}

std::vector<PropertyId> NamesToProperties(const std::vector<std::string> &property_names, DbAccessor &dba) {
  std::vector<PropertyId> properties;
  properties.reserve(property_names.size());

  for (const auto &name : property_names) {
    properties.push_back(dba.NameToProperty(name));
  }
  return properties;
}

std::vector<memgraph::storage::v3::LabelId> NamesToLabels(const std::vector<std::string> &label_names,
                                                          DbAccessor &dba) {
  std::vector<memgraph::storage::v3::LabelId> labels;
  labels.reserve(label_names.size());
  for (const auto &name : label_names) {
    labels.push_back(dba.NameToLabel(name));
  }
  return labels;
}

std::vector<PropertyId> GetPropertiesFromAcessors(
    const std::map<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue> &properties) {
  std::vector<PropertyId> ret_properties;
  ret_properties.reserve(properties.size());

  std::transform(properties.begin(), properties.end(), std::back_inserter(ret_properties),
                 [](const auto &prop) { return prop.first; });

  return ret_properties;
}

std::any ParseExpression(const std::string &expr, memgraph::expr::AstStorage &storage) {
  memgraph::frontend::opencypher::Parser<memgraph::frontend::opencypher::ParserOpTag::EXPRESSION> parser(expr);
  ParsingContext pc;
  CypherMainVisitor visitor(pc, &storage);

  auto *ast = parser.tree();
  return visitor.visit(ast);
}

TypedValue ComputeExpression(DbAccessor &dba, const std::optional<memgraph::storage::v3::VertexAccessor> &v_acc,
                             const std::optional<memgraph::storage::v3::EdgeAccessor> &e_acc,
                             const std::string &expression, std::string_view node_name, std::string_view edge_name) {
  AstStorage storage;
  Frame frame{128};
  SymbolTable symbol_table;
  EvaluationContext ctx;

  ExpressionEvaluator eval{&frame, symbol_table, ctx, &dba, View::OLD};
  auto expr = ParseExpression(expression, storage);

  auto node_identifier = Identifier(std::string(node_name), false);
  bool is_node_identifier_present = false;
  auto edge_identifier = Identifier(std::string(edge_name), false);
  bool is_edge_identifier_present = false;

  std::vector<Identifier *> identifiers;

  if (v_acc && expression.find(node_name) != std::string::npos) {
    is_node_identifier_present = true;
    identifiers.push_back(&node_identifier);
  }
  if (e_acc && expression.find(edge_name) != std::string::npos) {
    is_edge_identifier_present = true;
    identifiers.push_back(&edge_identifier);
  }

  expr::SymbolGenerator symbol_generator(&symbol_table, identifiers);
  (std::any_cast<Expression *>(expr))->Accept(symbol_generator);

  if (is_node_identifier_present) {
    frame[symbol_table.at(node_identifier)] = *v_acc;
  }
  if (is_edge_identifier_present) {
    frame[symbol_table.at(edge_identifier)] = *e_acc;
  }

  return Eval(std::any_cast<Expression *>(expr), ctx, storage, eval, dba);
}

}  // namespace memgraph::storage::v3
