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

#include <functional>
#include <iterator>
#include <utility>

#include "parser/opencypher/parser.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/bindings/ast/ast.hpp"
#include "storage/v3/bindings/cypher_main_visitor.hpp"
#include "storage/v3/bindings/db_accessor.hpp"
#include "storage/v3/bindings/eval.hpp"
#include "storage/v3/bindings/frame.hpp"
#include "storage/v3/bindings/symbol_generator.hpp"
#include "storage/v3/bindings/symbol_table.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/storage.hpp"
#include "storage/v3/value_conversions.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage/v3/view.hpp"

using memgraph::msgs::Label;
using memgraph::msgs::PropertyId;
using memgraph::msgs::Value;
using memgraph::msgs::Vertex;
using memgraph::msgs::VertexId;

using memgraph::storage::conversions::ConvertPropertyVector;
using memgraph::storage::conversions::ConvertValueVector;
using memgraph::storage::conversions::FromPropertyValueToValue;
using memgraph::storage::conversions::ToPropertyValue;
using memgraph::storage::v3::View;

namespace {
std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ConvertPropertyMap(
    std::vector<std::pair<PropertyId, Value>> &&properties) {
  std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ret;
  ret.reserve(properties.size());

  for (auto &[key, value] : properties) {
    ret.emplace_back(std::make_pair(key, ToPropertyValue(std::move(value))));
  }

  return ret;
}

std::vector<std::pair<memgraph::storage::v3::PropertyId, Value>> FromMap(
    const std::map<PropertyId, Value> &properties) {
  std::vector<std::pair<memgraph::storage::v3::PropertyId, Value>> ret;
  ret.reserve(properties.size());

  for (const auto &[key, value] : properties) {
    ret.emplace_back(std::make_pair(key, value));
  }

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectSpecificPropertiesFromAccessor(
    const memgraph::storage::v3::VertexAccessor &acc, const std::vector<memgraph::storage::v3::PropertyId> &props,
    View view) {
  std::map<PropertyId, Value> ret;

  for (const auto &prop : props) {
    auto result = acc.GetProperty(prop, view);
    if (result.HasError()) {
      spdlog::debug("Encountered an Error while trying to get a vertex property.");
      return std::nullopt;
    }
    auto &value = result.GetValue();
    if (value.IsNull()) {
      spdlog::debug("The specified property does not exist but it should");
      return std::nullopt;
    }
    ret.emplace(std::make_pair(prop, FromPropertyValueToValue(value)));
  }

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(
    const memgraph::storage::v3::VertexAccessor &acc, View view) {
  std::map<PropertyId, Value> ret;
  auto iter = acc.Properties(view);
  if (iter.HasError()) {
    spdlog::debug("Encountered an error while trying to get vertex properties.");
    return std::nullopt;
  }

  for (const auto &[prop_key, prop_val] : iter.GetValue()) {
    ret.emplace(prop_key, FromPropertyValueToValue(prop_val));
  }

  return ret;
}

Value ConstructValueVertex(const memgraph::storage::v3::VertexAccessor &acc, View view) {
  // Get the vertex id
  auto prim_label = acc.PrimaryLabel(view).GetValue();
  Label value_label{.id = prim_label};

  auto prim_key = ConvertValueVector(acc.PrimaryKey(view).GetValue());
  VertexId vertex_id = std::make_pair(value_label, prim_key);

  // Get the labels
  auto vertex_labels = acc.Labels(view).GetValue();
  std::vector<Label> value_labels;
  for (const auto &label : vertex_labels) {
    Label l = {.id = label};
    value_labels.push_back(l);
  }

  return Value({.id = vertex_id, .labels = value_labels});
}

Value ConstructValueEdge(const memgraph::storage::v3::EdgeAccessor &acc, View view) {
  memgraph::msgs::EdgeType type = {.id = acc.EdgeType().AsUint()};
  memgraph::msgs::EdgeId gid = {.gid = acc.Gid().AsUint()};

  Label src_prim_label = {.id = acc.FromVertex().primary_label};
  memgraph::msgs::VertexId src_vertex =
      std::make_pair(src_prim_label, ConvertValueVector(acc.FromVertex().primary_key));

  Label dst_prim_label = {.id = acc.ToVertex().primary_label};
  memgraph::msgs::VertexId dst_vertex = std::make_pair(dst_prim_label, ConvertValueVector(acc.ToVertex().primary_key));

  std::optional<std::vector<std::pair<PropertyId, Value>>> properties_opt = {};
  const auto &properties = acc.Properties(view);

  if (properties.HasValue()) {
    std::vector<std::pair<PropertyId, Value>> present_properties;
    present_properties.reserve(properties.GetValue().size());

    for (const auto &[prop_key, prop_val] : properties.GetValue()) {
      present_properties.emplace_back(std::make_pair(prop_key, FromPropertyValueToValue(prop_val)));
    }

    properties_opt = std::move(present_properties);
  }

  return Value({.src = src_vertex, .dst = dst_vertex, .properties = properties_opt, .id = gid, .type = type});
}

/// Conversion from TypedValue to Value
Value FromTypedValueToValue(memgraph::storage::v3::TypedValue &&tv) {
  using memgraph::storage::v3::TypedValue;

  switch (tv.type()) {
    case TypedValue::Type::Bool:
      return Value(tv.ValueBool());
    case TypedValue::Type::Double:
      return Value(tv.ValueDouble());
    case TypedValue::Type::Int:
      return Value(tv.ValueInt());
    case TypedValue::Type::List: {
      std::vector<Value> list;
      list.reserve(tv.ValueList().size());
      for (auto &elem : tv.ValueList()) {
        list.emplace_back(FromTypedValueToValue(std::move(elem)));
      }

      return Value(list);
    }
    case TypedValue::Type::Map: {
      std::map<std::string, Value> map;
      for (auto &[key, val] : tv.ValueMap()) {
        // maybe use std::make_pair once the && issue is resolved.
        map.emplace(key, FromTypedValueToValue(std::move(val)));
      }

      return Value(map);
    }
    case TypedValue::Type::Null:
      return Value{};
    case TypedValue::Type::String:
      return Value((std::string(tv.ValueString())));
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
      MG_ASSERT(false, "This conversion betweem TypedValue and Value is not implemented yet!");
      return Value{};
    }
  }
  return Value{};
}

std::vector<Value> ConvertToValueVectorFromTypedValueVector(std::vector<memgraph::storage::v3::TypedValue> &&vec) {
  std::vector<Value> ret;
  ret.reserve(vec.size());
  for (auto &&elem : vec) {
    ret.emplace_back(FromTypedValueToValue(std::move(elem)));
  }
  return ret;
}

std::vector<PropertyId> NamesToProperties(const std::vector<std::string> &property_names,
                                          memgraph::expr::DbAccessor &dba) {
  std::vector<PropertyId> properties;
  properties.reserve(property_names.size());
  for (const auto &name : property_names) {
    properties.push_back(dba.NameToProperty(name));
  }
  return properties;
}

std::vector<memgraph::storage::v3::LabelId> NamesToLabels(const std::vector<std::string> &label_names,
                                                          memgraph::expr::DbAccessor &dba) {
  std::vector<memgraph::storage::v3::LabelId> labels;
  labels.reserve(label_names.size());
  for (const auto &name : label_names) {
    labels.push_back(dba.NameToLabel(name));
  }
  return labels;
}

template <class TExpression>
auto Eval(TExpression *expr, memgraph::expr::EvaluationContext ctx, memgraph::expr::AstStorage &storage,
          memgraph::storage::v3::ExpressionEvaluator &eval, memgraph::expr::DbAccessor &dba) {
  ctx.properties = NamesToProperties(storage.properties_, dba);
  ctx.labels = NamesToLabels(storage.labels_, dba);
  auto value = expr->Accept(eval);
  return value;
}

memgraph::expr::EvaluationContext CreateEvaluationContext(const memgraph::storage::v3::VertexAccessor &v_acc) {
  memgraph::expr::EvaluationContext ctx;

  // Fill EvaluationContext up with properties
  const auto properties = v_acc.Properties(View::OLD);
  std::vector<PropertyId> properties_in_shard;

  for (const auto &prop : properties.GetValue()) {
    properties_in_shard.emplace_back(prop.first);
  }

  ctx.properties = std::move(properties_in_shard);

  // Fill EvaluationContext up with labels
  const auto labels = v_acc.Labels(View::OLD);
  std::vector<memgraph::storage::v3::LabelId> labels_in_shard;

  for (const auto &label : labels.GetValue()) {
    labels_in_shard.emplace_back(label);
  }

  ctx.labels = std::move(labels_in_shard);

  return ctx;
}

// TODO(gvolfing) Verify if edge_name is not needed here at all
bool FilterOnVertrex(memgraph::expr::DbAccessor &dba, const memgraph::storage::v3::VertexAccessor &v_acc,
                     const std::vector<std::string> filters, std::string_view node_name) {
  for (const auto &filter_expr : filters) {
    // Parse stuff
    memgraph::frontend::opencypher::Parser<memgraph::frontend::opencypher::ParserOpTag::EXPRESSION> parser(filter_expr);
    memgraph::expr::ParsingContext pc;
    memgraph::expr::AstStorage storage;
    memgraph::expr::CypherMainVisitor visitor(pc, &storage);

    memgraph::expr::Frame<memgraph::expr::TypedValue> frame{static_cast<int64_t>(128)};
    memgraph::expr::SymbolTable symbol_table;

    auto ctx = CreateEvaluationContext(v_acc);

    memgraph::storage::v3::ExpressionEvaluator eval{&frame, symbol_table, ctx, &dba, memgraph::expr::View::NEW};

    auto *ast = parser.tree();
    auto expr = visitor.visit(ast);

    auto node_identifier = memgraph::expr::Identifier(std::string(node_name), false);
    bool is_node_identifier_present = false;

    std::vector<memgraph::expr::Identifier *> identifiers;

    if (filter_expr.find(node_name) != std::string::npos) {
      is_node_identifier_present = true;
      identifiers.push_back(&node_identifier);
    }

    memgraph::expr::SymbolGenerator symbol_generator(&symbol_table, identifiers);
    (std::any_cast<memgraph::expr::Expression *>(expr))->Accept(symbol_generator);

    if (is_node_identifier_present) {
      frame[symbol_table.at(node_identifier)] = v_acc;
    }

    if (!Eval(std::any_cast<memgraph::expr::Expression *>(expr), ctx, storage, eval, dba).ValueBool()) {
      return false;
    }
  }

  return true;
}

std::vector<memgraph::storage::v3::TypedValue> EvaluateVertexExpressions(
    memgraph::expr::DbAccessor &dba, const memgraph::storage::v3::VertexAccessor &v_acc,
    const std::vector<std::string> expressions, std::string_view node_name) {
  std::vector<memgraph::storage::v3::TypedValue> evaluated_expressions;

  for (const auto &expression : expressions) {
    // Parse stuff
    memgraph::frontend::opencypher::Parser<memgraph::frontend::opencypher::ParserOpTag::EXPRESSION> parser(expression);
    memgraph::expr::ParsingContext pc;
    memgraph::expr::AstStorage storage;
    memgraph::expr::CypherMainVisitor visitor(pc, &storage);

    memgraph::expr::Frame<memgraph::expr::TypedValue> frame{static_cast<int64_t>(128)};
    memgraph::expr::SymbolTable symbol_table;

    auto ctx = CreateEvaluationContext(v_acc);

    memgraph::storage::v3::ExpressionEvaluator eval{&frame, symbol_table, ctx, &dba, memgraph::expr::View::NEW};

    auto *ast = parser.tree();
    auto expr = visitor.visit(ast);

    auto node_identifier = memgraph::expr::Identifier(std::string(node_name), false);
    bool is_node_identifier_present = false;

    std::vector<memgraph::expr::Identifier *> identifiers;

    // Does an expression allways contain the name of the vertex?
    if (expression.find(node_name) != std::string::npos) {
      is_node_identifier_present = true;
      identifiers.push_back(&node_identifier);
    }

    memgraph::expr::SymbolGenerator symbol_generator(&symbol_table, identifiers);
    (std::any_cast<memgraph::expr::Expression *>(expr))->Accept(symbol_generator);

    if (is_node_identifier_present) {
      frame[symbol_table.at(node_identifier)] = v_acc;
    }

    evaluated_expressions.emplace_back(
        Eval(std::any_cast<memgraph::expr::Expression *>(expr), ctx, storage, eval, dba));
  }
  return evaluated_expressions;
}

bool DoesEdgeTypeMatch(const memgraph::msgs::ExpandOneRequest &req, const memgraph::storage::v3::EdgeAccessor &edge) {
  // TODO(gvolfing) This should be checked only once and handled accordingly.
  if (req.edge_types.empty()) {
    return true;
  }

  return std::ranges::any_of(req.edge_types.cbegin(), req.edge_types.cend(),
                             [&edge](const memgraph::msgs::EdgeType &edge_type) {
                               return memgraph::storage::v3::EdgeTypeId::FromUint(edge_type.id) == edge.EdgeType();
                             });
}

struct LocalError {};

std::optional<memgraph::msgs::Vertex> FillUpSourceVertex(
    const std::optional<memgraph::storage::v3::VertexAccessor> &v_acc, memgraph::msgs::ExpandOneRequest &req,
    memgraph::msgs::VertexId src_vertex) {
  auto secondary_labels = v_acc->Labels(View::OLD);
  if (secondary_labels.HasError()) {
    spdlog::debug("Encountered an error while trying to get the secondary labels of a vertex. Transaction id: {}",
                  req.transaction_id.logical_id);
    return std::nullopt;
  }

  memgraph::msgs::Vertex source_vertex;
  source_vertex.id = src_vertex;
  source_vertex.labels.reserve(secondary_labels.GetValue().size());
  for (auto label_id : secondary_labels.GetValue()) {
    source_vertex.labels.emplace_back(memgraph::msgs::Label{.id = label_id});
  }
  return source_vertex;
}

std::optional<std::map<PropertyId, Value>> FillUpSourceVertexProperties(
    const std::optional<memgraph::storage::v3::VertexAccessor> &v_acc, memgraph::msgs::ExpandOneRequest &req) {
  std::map<PropertyId, Value> src_vertex_properties;

  if (!req.src_vertex_properties) {
    auto props = v_acc->Properties(View::OLD);
    if (props.HasError()) {
      spdlog::debug("Encountered an error while trying to access vertex properties. Transaction id: {}",
                    req.transaction_id.logical_id);
      return std::nullopt;
    }

    for (auto &[key, val] : props.GetValue()) {
      src_vertex_properties.insert(std::make_pair(key, FromPropertyValueToValue(val)));
    }

  } else if (req.src_vertex_properties.value().empty()) {
    // NOOP
  } else {
    for (const auto &prop : req.src_vertex_properties.value()) {
      const auto &prop_val = v_acc->GetProperty(prop, View::OLD);
      src_vertex_properties.insert(std::make_pair(prop, FromPropertyValueToValue(prop_val.GetValue())));
    }
  }

  return src_vertex_properties;
}

std::optional<std::array<std::vector<memgraph::storage::v3::EdgeAccessor>, 2>> FillUpConnectingEdges(
    const std::optional<memgraph::storage::v3::VertexAccessor> &v_acc, memgraph::msgs::ExpandOneRequest &req) {
  std::vector<memgraph::storage::v3::EdgeAccessor> in_edges;
  std::vector<memgraph::storage::v3::EdgeAccessor> out_edges;

  switch (req.direction) {
    case memgraph::msgs::EdgeDirection::OUT: {
      auto out_edges_result = v_acc->OutEdges(View::OLD);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      out_edges = std::move(out_edges_result.GetValue());
      break;
    }
    case memgraph::msgs::EdgeDirection::IN: {
      auto in_edges_result = v_acc->InEdges(View::OLD);
      if (in_edges_result.HasError()) {
        spdlog::debug(
            "Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}"[req.transaction_id
                                                                                                      .logical_id]);
        return std::nullopt;
      }
      in_edges = std::move(in_edges_result.GetValue());
      break;
    }
    case memgraph::msgs::EdgeDirection::BOTH: {
      auto in_edges_result = v_acc->InEdges(View::OLD);
      if (in_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      in_edges = std::move(in_edges_result.GetValue());

      auto out_edges_result = v_acc->OutEdges(View::OLD);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      out_edges = std::move(out_edges_result.GetValue());
      break;
    }
  }
  return std::array<std::vector<memgraph::storage::v3::EdgeAccessor>, 2>{in_edges, out_edges};
}

using AllEdgePropertyDataSructure = std::map<PropertyId, memgraph::msgs::Value>;
using SpecificEdgePropertyDataSructure = std::vector<memgraph::msgs::Value>;

using AllEdgeProperties = std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, AllEdgePropertyDataSructure>;
using SpecificEdgeProperties =
    std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, SpecificEdgePropertyDataSructure>;

using SpecificEdgePropertiesVector = std::vector<SpecificEdgeProperties>;
using AllEdgePropertiesVector = std::vector<AllEdgeProperties>;

template <typename ReturnType, typename EdgeProperties, typename EdgePropertyDataStructure, typename Functor>
std::optional<ReturnType> GetEdgesWithProperties(const std::vector<memgraph::storage::v3::EdgeAccessor> &edges,
                                                 const memgraph::msgs::ExpandOneRequest &req,
                                                 Functor get_edge_properties) {
  ReturnType ret;
  ret.reserve(edges.size());

  for (const auto &edge : edges) {
    if (!DoesEdgeTypeMatch(req, edge)) {
      continue;
    }

    EdgeProperties ret_tuple;

    memgraph::msgs::Label label;
    label.id = edge.FromVertex().primary_label;
    memgraph::msgs::VertexId other_vertex = std::make_pair(label, ConvertValueVector(edge.FromVertex().primary_key));

    const auto edge_props_var = get_edge_properties(edge);

    if (std::get_if<LocalError>(&edge_props_var) != nullptr) {
      return std::nullopt;
    }

    auto edge_props = std::get<EdgePropertyDataStructure>(edge_props_var);
    memgraph::msgs::Gid gid = edge.Gid().AsUint();

    ret.emplace_back(EdgeProperties{other_vertex, gid, edge_props});
  }

  return ret;
}

template <typename TPropertyValue, typename TPropertyNullopt>
void SetFinalEdgeProperties(std::optional<TPropertyValue> &properties_to_value,
                            std::optional<TPropertyNullopt> &properties_to_nullopt, const TPropertyValue &ret_out,
                            const TPropertyValue &ret_in, const memgraph::msgs::ExpandOneRequest &req) {
  switch (req.direction) {
    case memgraph::msgs::EdgeDirection::OUT: {
      properties_to_value = std::move(ret_out);
      break;
    }
    case memgraph::msgs::EdgeDirection::IN: {
      properties_to_value = std::move(ret_in);
      break;
    }
    case memgraph::msgs::EdgeDirection::BOTH: {
      TPropertyValue ret;
      ret.resize(ret_out.size() + ret_in.size());
      ret.insert(ret.end(), std::make_move_iterator(ret_in.begin()), std::make_move_iterator(ret_in.end()));
      ret.insert(ret.end(), std::make_move_iterator(ret_out.begin()), std::make_move_iterator(ret_out.end()));

      properties_to_value = ret;
      break;
    }
  }
  properties_to_nullopt = {};
}

std::optional<memgraph::msgs::ExpandOneResultRow> GetExpandOneResult(memgraph::storage::v3::Shard::Accessor &acc,
                                                                     memgraph::msgs::VertexId src_vertex,
                                                                     memgraph::msgs::ExpandOneRequest req) {
  using EdgeProperties =
      std::variant<LocalError, std::map<PropertyId, memgraph::msgs::Value>, std::vector<memgraph::msgs::Value>>;
  std::function<EdgeProperties(const memgraph::storage::v3::EdgeAccessor &)> get_edge_properties;

  if (!req.edge_properties) {
    get_edge_properties = [&req](const memgraph::storage::v3::EdgeAccessor &edge) -> EdgeProperties {
      std::map<PropertyId, memgraph::msgs::Value> ret;
      auto property_results = edge.Properties(View::OLD);
      if (property_results.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return LocalError{};
      }

      for (const auto &[prop_key, prop_val] : property_results.GetValue()) {
        ret.insert(std::make_pair(prop_key, FromPropertyValueToValue(prop_val)));
      }
      return ret;
    };
  } else {
    // TODO(gvolfing) - do we want to set the action_successful here?
    get_edge_properties = [&req](const memgraph::storage::v3::EdgeAccessor &edge) {
      std::vector<memgraph::msgs::Value> ret;
      ret.reserve(req.edge_properties.value().size());
      for (const auto &edge_prop : req.edge_properties.value()) {
        // TODO(gvolfing) maybe check for the absence of certain properties
        ret.emplace_back(FromPropertyValueToValue(edge.GetProperty(edge_prop, View::OLD).GetValue()));
      }
      return ret;
    };
  }

  /// Fill up source vertex
  auto v_acc = acc.FindVertex(ConvertPropertyVector(std::move(src_vertex.second)), View::OLD);

  auto source_vertex = FillUpSourceVertex(v_acc, req, src_vertex);
  if (!source_vertex) {
    return std::nullopt;
  }

  /// Fill up source vertex properties
  auto src_vertex_properties = FillUpSourceVertexProperties(v_acc, req);
  if (!src_vertex_properties) {
    return std::nullopt;
  }

  /// Fill up connecting edges
  auto fill_up_connecting_edges = FillUpConnectingEdges(v_acc, req);
  if (!fill_up_connecting_edges) {
    return std::nullopt;
  }

  auto [in_edges, out_edges] = fill_up_connecting_edges.value();

  /// Assemble the edge properties
  std::optional<AllEdgePropertiesVector> edges_with_all_properties;
  std::optional<SpecificEdgePropertiesVector> edges_with_specific_properties;

  if (!req.edge_properties) {
    auto ret_in_opt = GetEdgesWithProperties<AllEdgePropertiesVector, AllEdgeProperties, AllEdgePropertyDataSructure>(
        in_edges, req, get_edge_properties);
    if (!ret_in_opt) {
      return std::nullopt;
    }

    auto ret_out_opt = GetEdgesWithProperties<AllEdgePropertiesVector, AllEdgeProperties, AllEdgePropertyDataSructure>(
        out_edges, req, get_edge_properties);
    if (!ret_out_opt) {
      return std::nullopt;
    }

    auto &ret_in = *ret_in_opt;
    auto &ret_out = *ret_out_opt;

    SetFinalEdgeProperties<AllEdgePropertiesVector, SpecificEdgePropertiesVector>(
        edges_with_all_properties, edges_with_specific_properties, ret_out, ret_in, req);
  } else {
    auto ret_in_opt =
        GetEdgesWithProperties<SpecificEdgePropertiesVector, SpecificEdgeProperties, SpecificEdgePropertyDataSructure>(
            in_edges, req, get_edge_properties);
    if (!ret_in_opt) {
      return std::nullopt;
    }

    auto ret_out_opt =
        GetEdgesWithProperties<SpecificEdgePropertiesVector, SpecificEdgeProperties, SpecificEdgePropertyDataSructure>(
            out_edges, req, get_edge_properties);
    if (!ret_out_opt) {
      return std::nullopt;
    }

    auto &ret_in = *ret_in_opt;
    auto &ret_out = *ret_out_opt;

    SetFinalEdgeProperties<SpecificEdgePropertiesVector, AllEdgePropertiesVector>(
        edges_with_specific_properties, edges_with_all_properties, ret_out, ret_in, req);
  }

  return memgraph::msgs::ExpandOneResultRow{
      .src_vertex = std::move(*source_vertex),
      .src_vertex_properties = std::move(src_vertex_properties),
      .edges_with_all_properties = std::move(edges_with_all_properties),
      .edges_with_specific_properties = std::move(edges_with_specific_properties)};
}

}  // namespace

namespace memgraph::storage::v3 {

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  // Workaround untill we have access to CreateVertexAndValidate()
  // with the new signature that does not require the primary label.
  const auto prim_label = acc.GetPrimaryLabel();

  bool action_successful = true;

  for (auto &new_vertex : req.new_vertices) {
    /// TODO(gvolfing) Remove this. In the new implementation each shard
    /// should have a predetermined primary label, so there is no point in
    /// specifying it in the accessor functions. Their signature will
    /// change.
    /// TODO(gvolfing) Consider other methods than converting. Change either
    /// the way that the property map is stored in the messages, or the
    /// signature of CreateVertexAndValidate.
    auto converted_property_map = ConvertPropertyMap(std::move(new_vertex.properties));

    // TODO(gvolfing) make sure if this conversion is actually needed.
    std::vector<memgraph::storage::v3::LabelId> converted_label_ids;
    converted_label_ids.reserve(new_vertex.label_ids.size());
    for (const auto &label_id : new_vertex.label_ids) {
      converted_label_ids.emplace_back(label_id.id);
    }

    auto result_schema =
        acc.CreateVertexAndValidate(prim_label, converted_label_ids,
                                    ConvertPropertyVector(std::move(new_vertex.primary_key)), converted_property_map);

    if (result_schema.HasError()) {
      auto &error = result_schema.GetError();

      std::visit(
          []<typename T>(T &&) {
            using ErrorType = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<ErrorType, SchemaViolation>) {
              spdlog::debug("Creating vertex failed with error: SchemaViolation");
            } else if constexpr (std::is_same_v<ErrorType, Error>) {
              spdlog::debug("Creating vertex failed with error: Error");
            } else {
              static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
            }
          },
          error);

      action_successful = false;
      break;
    }
  }

  return memgraph::msgs::CreateVerticesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  bool action_successful = true;

  for (auto &vertex : req.new_properties) {
    if (!action_successful) {
      break;
    }

    auto vertex_to_update = acc.FindVertex(ConvertPropertyVector(std::move(vertex.primary_key)), View::OLD);
    if (!vertex_to_update) {
      action_successful = false;
      spdlog::debug("Vertex could not be found while trying to update its properties. Transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }

    for (auto &update_prop : vertex.property_updates) {
      // TODO(gvolfing) Maybe check if the setting is valid if SetPropertyAndValidate()
      // does not do that alreaedy.
      auto result_schema =
          vertex_to_update->SetPropertyAndValidate(update_prop.first, ToPropertyValue(std::move(update_prop.second)));
      if (result_schema.HasError()) {
        auto &error = result_schema.GetError();

        std::visit(
            [&action_successful]<typename T>(T &&) {
              using ErrorType = std::remove_cvref_t<T>;
              if constexpr (std::is_same_v<ErrorType, SchemaViolation>) {
                action_successful = false;
                spdlog::debug("Updating vertex failed with error: SchemaViolation");
              } else if constexpr (std::is_same_v<ErrorType, Error>) {
                action_successful = false;
                spdlog::debug("Updating vertex failed with error: Error");
              } else {
                static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
              }
            },
            error);

        break;
      }
    }
  }

  return memgraph::msgs::UpdateVerticesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::DeleteVerticesRequest &&req) {
  bool action_successful = true;
  auto acc = shard_->Access(req.transaction_id);

  for (auto &propval : req.primary_keys) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(std::move(propval)), View::OLD);

    if (!vertex_acc) {
      spdlog::debug("Error while trying to delete vertex. Vertex to delete does not exist. Transaction id: {}",
                    req.transaction_id.logical_id);
      action_successful = false;
    } else {
      // TODO(gvolfing)
      // Since we will not have different kinds of deletion types in one transaction,
      // we dont have to enter the switch statement on every iteration. Optimize this.
      switch (req.deletion_type) {
        case memgraph::msgs::DeleteVerticesRequest::DeletionType::DELETE: {
          auto result = acc.DeleteVertex(&vertex_acc.value());
          if (result.HasError() || !(result.GetValue().has_value())) {
            action_successful = false;
            spdlog::debug("Error while trying to delete vertex. Transaction id: {}", req.transaction_id.logical_id);
          }

          break;
        }
        case memgraph::msgs::DeleteVerticesRequest::DeletionType::DETACH_DELETE: {
          auto result = acc.DetachDeleteVertex(&vertex_acc.value());
          if (result.HasError() || !(result.GetValue().has_value())) {
            action_successful = false;
            spdlog::debug("Error while trying to detach and delete vertex. Transaction id: {}",
                          req.transaction_id.logical_id);
          }

          break;
        }
      }
    }
  }

  return memgraph::msgs::DeleteVerticesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateEdgesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  bool action_successful = true;

  for (auto &edge : req.edges) {
    auto vertex_acc_from_primary_key = edge.src.second;
    auto vertex_from_acc = acc.FindVertex(ConvertPropertyVector(std::move(vertex_acc_from_primary_key)), View::OLD);

    auto vertex_acc_to_primary_key = edge.dst.second;
    auto vertex_to_acc = acc.FindVertex(ConvertPropertyVector(std::move(vertex_acc_to_primary_key)), View::OLD);

    if (!vertex_from_acc || !vertex_to_acc) {
      action_successful = false;
      spdlog::debug("Error while trying to insert edge, vertex does not exist. Transaction id: {}",
                    req.transaction_id.logical_id);
      break;
    }

    auto from_vertex_id = VertexId(edge.src.first.id, ConvertPropertyVector(std::move(edge.src.second)));
    auto to_vertex_id = VertexId(edge.dst.first.id, ConvertPropertyVector(std::move(edge.dst.second)));
    auto edge_acc =
        acc.CreateEdge(from_vertex_id, to_vertex_id, EdgeTypeId::FromUint(edge.type.id), Gid::FromUint(edge.id.gid));

    if (edge_acc.HasError()) {
      action_successful = false;
      spdlog::debug("Creating edge was not successful. Transaction id: {}", req.transaction_id.logical_id);
      break;
    }

    // Add properties to the edge if there is any
    if (edge.properties) {
      for (auto &[edge_prop_key, edge_prop_val] : edge.properties.value()) {
        auto set_result = edge_acc->SetProperty(edge_prop_key, ToPropertyValue(std::move(edge_prop_val)));
        if (set_result.HasError()) {
          action_successful = false;
          spdlog::debug("Adding property to edge was not successful. Transaction id: {}",
                        req.transaction_id.logical_id);
          break;
        }
      }
    }
  }

  return memgraph::msgs::CreateEdgesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::DeleteEdgesRequest &&req) {
  bool action_successful = true;
  auto acc = shard_->Access(req.transaction_id);

  for (auto &edge : req.edges) {
    if (!action_successful) {
      break;
    }

    auto edge_acc = acc.DeleteEdge(VertexId(edge.src.first.id, ConvertPropertyVector(std::move(edge.src.second))),
                                   VertexId(edge.dst.first.id, ConvertPropertyVector(std::move(edge.dst.second))),
                                   Gid::FromUint(edge.id.gid));
    if (edge_acc.HasError() || !edge_acc.HasValue()) {
      spdlog::debug("Error while trying to delete edge. Transaction id: {}", req.transaction_id.logical_id);
      action_successful = false;
      continue;
    }
  }

  return memgraph::msgs::DeleteEdgesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateEdgesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  bool action_successful = true;

  for (auto &edge : req.new_properties) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(std::move(edge.src.second)), View::OLD);
    if (!vertex_acc) {
      action_successful = false;
      spdlog::debug("Encountered an error while trying to acquire VertexAccessor with transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }

    // Since we are using the source vertex of the edge we are only intrested
    // in the vertex's out-going edges
    auto edges_res = vertex_acc->OutEdges(View::OLD);
    if (edges_res.HasError()) {
      action_successful = false;
      spdlog::debug("Encountered an error while trying to acquire EdgeAccessor with transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }

    auto &edge_accessors = edges_res.GetValue();

    // Look for the appropriate edge accessor
    bool edge_accessor_did_match = false;
    for (auto &edge_accessor : edge_accessors) {
      if (edge_accessor.Gid().AsUint() == edge.edge_id.gid) {  // Found the appropriate accessor
        edge_accessor_did_match = true;
        for (auto &[key, value] : edge.property_updates) {
          // TODO(gvolfing)
          // Check if the property was set if SetProperty does not do that itself.
          auto res = edge_accessor.SetProperty(key, ToPropertyValue(std::move(value)));
          if (res.HasError()) {
            spdlog::debug("Encountered an error while trying to set the property of an Edge with transaction id: {}",
                          req.transaction_id.logical_id);
          }
        }
      }
    }

    if (!edge_accessor_did_match) {
      action_successful = false;
      spdlog::debug("Could not find the Edge with the specified Gid. Transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }
  }

  return memgraph::msgs::UpdateEdgesResponse{.success = action_successful};
}

msgs::ReadResponses ShardRsm::HandleRead(msgs::ScanVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  bool action_successful = true;

  std::vector<memgraph::msgs::ScanResultRow> results;
  std::optional<memgraph::msgs::VertexId> next_start_id;

  const auto view = View(req.storage_view);
  auto vertex_iterable = acc.Vertices(view);
  bool did_reach_starting_point = false;
  uint64_t sample_counter = 0;

  const auto start_ids = ConvertPropertyVector(std::move(req.start_id.second));

  for (auto it = vertex_iterable.begin(); it != vertex_iterable.end(); ++it) {
    const auto &vertex = *it;

    if (start_ids == vertex.PrimaryKey(View(req.storage_view)).GetValue()) {
      did_reach_starting_point = true;
    }

    if (did_reach_starting_point) {
      std::vector<Value> expression_results;

      // TODO(gvolfing) it should be enough to check this only once.
      if (req.filter_expressions) {
        // NOTE - DbAccessor might get removed in the future.
        auto dba = DbAccessor{&acc};
        const bool eval = FilterOnVertrex(dba, vertex, req.filter_expressions.value(), node_name_);
        if (!eval) {
          continue;
        }
      }
      if (req.vertex_expressions) {
        // NOTE - DbAccessor might get removed in the future.
        auto dba = DbAccessor{&acc};
        expression_results = ConvertToValueVectorFromTypedValueVector(
            EvaluateVertexExpressions(dba, vertex, req.vertex_expressions.value(), node_name_));
      }

      std::optional<std::map<PropertyId, Value>> found_props;

      if (req.props_to_return) {
        found_props = CollectSpecificPropertiesFromAccessor(vertex, req.props_to_return.value(), view);
      } else {
        found_props = CollectAllPropertiesFromAccessor(vertex, view);
      }

      // TODO(gvolfing) -VERIFY-
      // Vertex is seperated from the properties in the response.
      // Is it useful to return just a vertex without the properties?
      if (!found_props) {
        action_successful = false;
        break;
      }

      results.emplace_back(msgs::ScanResultRow{.vertex = ConstructValueVertex(vertex, view).vertex_v,
                                               .props = FromMap(found_props.value()),
                                               .evaluated_vertex_expressions = std::move(expression_results)});

      ++sample_counter;
      if (sample_counter == req.batch_limit) {
        // Reached the maximum specified batch size.
        // Get the next element before exiting.
        const auto &next_vertex = *(++it);
        next_start_id = ConstructValueVertex(next_vertex, view).vertex_v.id;

        break;
      }
    }
  }

  memgraph::msgs::ScanVerticesResponse resp{};
  resp.success = action_successful;

  if (action_successful) {
    resp.next_start_id = next_start_id;
    resp.results = std::move(results);
  }

  return resp;
}

msgs::ReadResponses ShardRsm::HandleRead(msgs::ExpandOneRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  bool action_successful = true;

  std::vector<memgraph::msgs::ExpandOneResultRow> results;

  for (auto &src_vertex : req.src_vertices) {
    auto result = GetExpandOneResult(acc, src_vertex, req);

    if (!result) {
      action_successful = false;
      break;
    }

    results.emplace_back(result.value());
  }

  memgraph::msgs::ExpandOneResponse resp{};
  if (action_successful) {
    resp.result = std::move(results);
  }

  return resp;
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CommitRequest &&req) {
  shard_->Access(req.transaction_id).Commit(req.commit_timestamp);
  return memgraph::msgs::CommitResponse{true};
};

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
msgs::ReadResponses ShardRsm::HandleRead(msgs::GetPropertiesRequest && /*req*/) {
  return memgraph::msgs::GetPropertiesResponse{};
}

}  //    namespace memgraph::storage::v3
