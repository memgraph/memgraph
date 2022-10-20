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

#include <algorithm>
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
#include "storage/v3/bindings/pretty_print_ast_to_original_expression.hpp"
#include "storage/v3/bindings/symbol_generator.hpp"
#include "storage/v3/bindings/symbol_table.hpp"
#include "storage/v3/bindings/typed_value.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/storage.hpp"
#include "storage/v3/value_conversions.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage/v3/vertex_id.hpp"
#include "storage/v3/view.hpp"

namespace memgraph::storage::v3 {
using msgs::Label;
using msgs::PropertyId;
using msgs::Value;

using conversions::ConvertPropertyVector;
using conversions::ConvertValueVector;
using conversions::FromPropertyValueToValue;
using conversions::ToMsgsVertexId;
using conversions::ToPropertyValue;

namespace {
namespace msgs = msgs;

std::vector<std::pair<PropertyId, PropertyValue>> ConvertPropertyMap(
    std::vector<std::pair<PropertyId, Value>> &&properties) {
  std::vector<std::pair<PropertyId, PropertyValue>> ret;
  ret.reserve(properties.size());

  std::transform(std::make_move_iterator(properties.begin()), std::make_move_iterator(properties.end()),
                 std::back_inserter(ret), [](std::pair<PropertyId, Value> &&property) {
                   return std::make_pair(property.first, ToPropertyValue(std::move(property.second)));
                 });

  return ret;
}

std::vector<std::pair<PropertyId, Value>> FromMap(const std::map<PropertyId, Value> &properties) {
  std::vector<std::pair<PropertyId, Value>> ret;
  ret.reserve(properties.size());

  std::transform(properties.begin(), properties.end(), std::back_inserter(ret),
                 [](const auto &property) { return std::make_pair(property.first, property.second); });

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectSpecificPropertiesFromAccessor(const VertexAccessor &acc,
                                                                                 const std::vector<PropertyId> &props,
                                                                                 View view) {
  std::map<PropertyId, Value> ret;

  for (const auto &prop : props) {
    auto result = acc.GetProperty(prop, view);
    if (result.HasError()) {
      spdlog::debug("Encountered an Error while trying to get a vertex property.");
      return std::nullopt;
    }
    auto &value = result.GetValue();
    ret.emplace(std::make_pair(prop, FromPropertyValueToValue(std::move(value))));
  }

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(const VertexAccessor &acc, View view,
                                                                            const Schemas::Schema *schema) {
  std::map<PropertyId, Value> ret;
  auto props = acc.Properties(view);
  if (props.HasError()) {
    spdlog::debug("Encountered an error while trying to get vertex properties.");
    return std::nullopt;
  }

  auto &properties = props.GetValue();
  std::transform(properties.begin(), properties.end(), std::inserter(ret, ret.begin()),
                 [](std::pair<const PropertyId, PropertyValue> &pair) {
                   return std::make_pair(pair.first, FromPropertyValueToValue(std::move(pair.second)));
                 });
  properties.clear();

  // TODO(antaljanosbenjamin): Once the VertexAccessor::Properties returns also the primary keys, we can get rid of this
  // code.
  auto maybe_pk = acc.PrimaryKey(view);
  if (maybe_pk.HasError()) {
    spdlog::debug("Encountered an error while trying to get vertex primary key.");
  }

  auto &pk = maybe_pk.GetValue();
  MG_ASSERT(schema->second.size() == pk.size(), "PrimaryKey size does not match schema!");
  for (size_t i{0}; i < schema->second.size(); ++i) {
    ret.emplace(schema->second[i].property_id, FromPropertyValueToValue(std::move(pk[i])));
  }

  return ret;
}

msgs::Value ConstructValueVertex(const VertexAccessor &acc, View view) {
  // Get the vertex id
  auto prim_label = acc.PrimaryLabel(view).GetValue();
  msgs::Label value_label{.id = prim_label};

  auto prim_key = ConvertValueVector(acc.PrimaryKey(view).GetValue());
  msgs::VertexId vertex_id = std::make_pair(value_label, prim_key);

  // Get the labels
  auto vertex_labels = acc.Labels(view).GetValue();
  std::vector<msgs::Label> value_labels;
  value_labels.reserve(vertex_labels.size());

  std::transform(vertex_labels.begin(), vertex_labels.end(), std::back_inserter(value_labels),
                 [](const auto &label) { return msgs::Label{.id = label}; });

  return Value({.id = vertex_id, .labels = value_labels});
}

Value ConstructValueEdge(const EdgeAccessor &acc, View view) {
  msgs::EdgeType type = {.id = acc.EdgeType()};
  msgs::EdgeId gid = {.gid = acc.Gid().AsUint()};

  Label src_prim_label = {.id = acc.FromVertex().primary_label};
  msgs::VertexId src_vertex = std::make_pair(src_prim_label, ConvertValueVector(acc.FromVertex().primary_key));

  Label dst_prim_label = {.id = acc.ToVertex().primary_label};
  msgs::VertexId dst_vertex = std::make_pair(dst_prim_label, ConvertValueVector(acc.ToVertex().primary_key));

  auto properties = acc.Properties(view);

  std::vector<std::pair<PropertyId, Value>> present_properties;
  if (properties.HasValue()) {
    auto &props = properties.GetValue();
    present_properties.reserve(props.size());

    std::transform(props.begin(), props.end(), std::back_inserter(present_properties),
                   [](std::pair<const PropertyId, PropertyValue> &prop) {
                     return std::make_pair(prop.first, FromPropertyValueToValue(std::move(prop.second)));
                   });
  }

  return Value(msgs::Edge{.src = std::move(src_vertex),
                          .dst = std::move(dst_vertex),
                          .properties = std::move(present_properties),
                          .id = gid,
                          .type = type});
}

Value FromTypedValueToValue(TypedValue &&tv) {
  switch (tv.type()) {
    case TypedValue::Type::Bool:
      return Value(tv.ValueBool());
    case TypedValue::Type::Double:
      return Value(tv.ValueDouble());
    case TypedValue::Type::Int:
      return Value(tv.ValueInt());
    case TypedValue::Type::List: {
      std::vector<Value> list;
      auto &tv_list = tv.ValueList();
      list.reserve(tv_list.size());
      std::transform(tv_list.begin(), tv_list.end(), std::back_inserter(list),
                     [](auto &elem) { return FromTypedValueToValue(std::move(elem)); });
      return Value(list);
    }
    case TypedValue::Type::Map: {
      std::map<std::string, Value> map;
      for (auto &[key, val] : tv.ValueMap()) {
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
      MG_ASSERT(false, "This conversion between TypedValue and Value is not implemented yet!");
      break;
    }
  }
  return Value{};
}

std::vector<Value> ConvertToValueVectorFromTypedValueVector(std::vector<TypedValue> &&vec) {
  std::vector<Value> ret;
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

std::vector<LabelId> NamesToLabels(const std::vector<std::string> &label_names, DbAccessor &dba) {
  std::vector<LabelId> labels;
  labels.reserve(label_names.size());
  for (const auto &name : label_names) {
    labels.push_back(dba.NameToLabel(name));
  }
  return labels;
}

template <class TExpression>
auto Eval(TExpression *expr, EvaluationContext &ctx, AstStorage &storage, ExpressionEvaluator &eval, DbAccessor &dba) {
  ctx.properties = NamesToProperties(storage.properties_, dba);
  ctx.labels = NamesToLabels(storage.labels_, dba);
  auto value = expr->Accept(eval);
  return value;
}

std::any ParseExpression(const std::string &expr, memgraph::expr::AstStorage &storage) {
  memgraph::frontend::opencypher::Parser<memgraph::frontend::opencypher::ParserOpTag::EXPRESSION> parser(expr);
  ParsingContext pc;
  CypherMainVisitor visitor(pc, &storage);

  auto *ast = parser.tree();
  return visitor.visit(ast);
}

TypedValue ComputeExpression(DbAccessor &dba, const std::optional<VertexAccessor> &v_acc,
                             const std::optional<EdgeAccessor> &e_acc, const std::string &expression,
                             std::string_view node_name, std::string_view edge_name) {
  AstStorage storage;
  Frame frame{1 + 1};  // 1 for the node_identifier, 1 for the edge_identifier
  SymbolTable symbol_table;
  EvaluationContext ctx;

  ExpressionEvaluator eval{&frame, symbol_table, ctx, &dba, View::OLD};
  auto expr = ParseExpression(expression, storage);

  auto node_identifier = Identifier(std::string(node_name), false);
  auto edge_identifier = Identifier(std::string(edge_name), false);

  std::vector<Identifier *> identifiers;
  identifiers.push_back(&node_identifier);
  identifiers.push_back(&edge_identifier);

  expr::SymbolGenerator symbol_generator(&symbol_table, identifiers);
  (std::any_cast<Expression *>(expr))->Accept(symbol_generator);

  if (node_identifier.symbol_pos_ != -1) {
    MG_ASSERT(std::find_if(symbol_table.table().begin(), symbol_table.table().end(),
                           [&node_name](const std::pair<int32_t, Symbol> &position_symbol_pair) {
                             return position_symbol_pair.second.name() == node_name;
                           }) != symbol_table.table().end());

    frame[symbol_table.at(node_identifier)] = *v_acc;
  }

  if (edge_identifier.symbol_pos_ != -1) {
    MG_ASSERT(std::find_if(symbol_table.table().begin(), symbol_table.table().end(),
                           [&edge_name](const std::pair<int32_t, Symbol> &position_symbol_pair) {
                             return position_symbol_pair.second.name() == edge_name;
                           }) != symbol_table.table().end());

    frame[symbol_table.at(edge_identifier)] = *e_acc;
  }

  return Eval(std::any_cast<Expression *>(expr), ctx, storage, eval, dba);
}

bool FilterOnVertex(DbAccessor &dba, const VertexAccessor &v_acc, const std::vector<std::string> &filters,
                    const std::string_view node_name) {
  return std::ranges::all_of(filters, [&node_name, &dba, &v_acc](const auto &filter_expr) {
    auto res = ComputeExpression(dba, v_acc, std::nullopt, filter_expr, node_name, "");
    return res.IsBool() && res.ValueBool();
  });
}

std::vector<TypedValue> EvaluateVertexExpressions(DbAccessor &dba, const VertexAccessor &v_acc,
                                                  const std::vector<std::string> &expressions,
                                                  std::string_view node_name) {
  std::vector<TypedValue> evaluated_expressions;
  evaluated_expressions.reserve(expressions.size());

  std::transform(expressions.begin(), expressions.end(), std::back_inserter(evaluated_expressions),
                 [&dba, &v_acc, &node_name](const auto &expression) {
                   return ComputeExpression(dba, v_acc, std::nullopt, expression, node_name, "");
                 });

  return evaluated_expressions;
}

bool DoesEdgeTypeMatch(const std::vector<msgs::EdgeType> &edge_types, const EdgeAccessor &edge) {
  // TODO(gvolfing) This should be checked only once and handled accordingly.
  if (edge_types.empty()) {
    return true;
  }

  return std::ranges::any_of(edge_types.begin(), edge_types.end(),
                             [&edge](const msgs::EdgeType &edge_type) { return edge_type.id == edge.EdgeType(); });
}

struct LocalError {};

std::optional<msgs::Vertex> FillUpSourceVertex(const std::optional<VertexAccessor> &v_acc,
                                               const msgs::ExpandOneRequest &req, msgs::VertexId src_vertex) {
  auto secondary_labels = v_acc->Labels(View::NEW);
  if (secondary_labels.HasError()) {
    spdlog::debug("Encountered an error while trying to get the secondary labels of a vertex. Transaction id: {}",
                  req.transaction_id.logical_id);
    return std::nullopt;
  }

  auto &sec_labels = secondary_labels.GetValue();
  msgs::Vertex source_vertex;
  source_vertex.id = src_vertex;
  source_vertex.labels.reserve(sec_labels.size());

  std::transform(sec_labels.begin(), sec_labels.end(), std::back_inserter(source_vertex.labels),
                 [](auto label_id) { return msgs::Label{.id = label_id}; });

  return source_vertex;
}

std::optional<std::map<PropertyId, Value>> FillUpSourceVertexProperties(const std::optional<VertexAccessor> &v_acc,
                                                                        const msgs::ExpandOneRequest &req) {
  std::map<PropertyId, Value> src_vertex_properties;

  if (!req.src_vertex_properties) {
    auto props = v_acc->Properties(View::NEW);
    if (props.HasError()) {
      spdlog::debug("Encountered an error while trying to access vertex properties. Transaction id: {}",
                    req.transaction_id.logical_id);
      return std::nullopt;
    }

    for (auto &[key, val] : props.GetValue()) {
      src_vertex_properties.insert(std::make_pair(key, FromPropertyValueToValue(std::move(val))));
    }

  } else if (req.src_vertex_properties.value().empty()) {
    // NOOP
  } else {
    for (const auto &prop : req.src_vertex_properties.value()) {
      auto prop_val = v_acc->GetProperty(prop, View::OLD);
      if (prop_val.HasError()) {
        spdlog::debug("Encountered an error while trying to access vertex properties. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      src_vertex_properties.insert(std::make_pair(prop, FromPropertyValueToValue(std::move(prop_val.GetValue()))));
    }
  }

  return src_vertex_properties;
}

std::optional<std::array<std::vector<EdgeAccessor>, 2>> FillUpConnectingEdges(
    const std::optional<VertexAccessor> &v_acc, const msgs::ExpandOneRequest &req) {
  std::vector<EdgeAccessor> in_edges;
  std::vector<EdgeAccessor> out_edges;

  switch (req.direction) {
    case msgs::EdgeDirection::OUT: {
      auto out_edges_result = v_acc->OutEdges(View::NEW);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      out_edges = std::move(out_edges_result.GetValue());
      break;
    }
    case msgs::EdgeDirection::IN: {
      auto in_edges_result = v_acc->InEdges(View::NEW);
      if (in_edges_result.HasError()) {
        spdlog::debug(
            "Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}"[req.transaction_id
                                                                                                      .logical_id]);
        return std::nullopt;
      }
      in_edges = std::move(in_edges_result.GetValue());
      break;
    }
    case msgs::EdgeDirection::BOTH: {
      auto in_edges_result = v_acc->InEdges(View::NEW);
      if (in_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      in_edges = std::move(in_edges_result.GetValue());

      auto out_edges_result = v_acc->OutEdges(View::NEW);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      out_edges = std::move(out_edges_result.GetValue());
      break;
    }
  }
  return std::array<std::vector<EdgeAccessor>, 2>{in_edges, out_edges};
}

using AllEdgePropertyDataSructure = std::map<PropertyId, msgs::Value>;
using SpecificEdgePropertyDataSructure = std::vector<msgs::Value>;

using AllEdgeProperties = std::tuple<msgs::VertexId, msgs::Gid, AllEdgePropertyDataSructure>;
using SpecificEdgeProperties = std::tuple<msgs::VertexId, msgs::Gid, SpecificEdgePropertyDataSructure>;

using SpecificEdgePropertiesVector = std::vector<SpecificEdgeProperties>;
using AllEdgePropertiesVector = std::vector<AllEdgeProperties>;

using EdgeFiller = std::function<bool(const EdgeAccessor &edge, bool is_in_edge, msgs::ExpandOneResultRow &result_row)>;

template <bool are_in_edges>
bool FillEdges(const std::vector<EdgeAccessor> &edges, const msgs::ExpandOneRequest &req, msgs::ExpandOneResultRow &row,
               const EdgeFiller &edge_filler) {
  for (const auto &edge : edges) {
    if (!DoesEdgeTypeMatch(req.edge_types, edge)) {
      continue;
    }

    if (!edge_filler(edge, are_in_edges, row)) {
      return false;
    }
  }

  return true;
}

std::optional<msgs::ExpandOneResultRow> GetExpandOneResult(Shard::Accessor &acc, msgs::VertexId src_vertex,
                                                           const msgs::ExpandOneRequest &req) {
  EdgeFiller edge_filler;

  if (!req.edge_properties) {
    edge_filler = [transaction_id = req.transaction_id.logical_id](const EdgeAccessor &edge, const bool is_in_edge,
                                                                   msgs::ExpandOneResultRow &result_row) -> bool {
      auto properties_results = edge.Properties(View::NEW);
      if (properties_results.HasError()) {
        spdlog::debug("Encountered an error while trying to get edge properties. Transaction id: {}", transaction_id);
        return false;
      }

      std::map<PropertyId, msgs::Value> value_properties;
      for (auto &[prop_key, prop_val] : properties_results.GetValue()) {
        value_properties.insert(std::make_pair(prop_key, FromPropertyValueToValue(std::move(prop_val))));
      }
      using EdgeWithAllProperties = msgs::ExpandOneResultRow::EdgeWithAllProperties;
      EdgeWithAllProperties edges{ToMsgsVertexId(edge.FromVertex()), msgs::EdgeType{edge.EdgeType()},
                                  edge.Gid().AsUint(), std::move(value_properties)};
      if (is_in_edge) {
        result_row.in_edges_with_all_properties.push_back(std::move(edges));
      } else {
        result_row.out_edges_with_all_properties.push_back(std::move(edges));
      }
      return true;
    };
  } else {
    // TODO(gvolfing) - do we want to set the action_successful here?
    edge_filler = [&req](const EdgeAccessor &edge, const bool is_in_edge,
                         msgs::ExpandOneResultRow &result_row) -> bool {
      std::vector<msgs::Value> value_properties;
      value_properties.reserve(req.edge_properties.value().size());
      for (const auto &edge_prop : req.edge_properties.value()) {
        auto property_result = edge.GetProperty(edge_prop, View::NEW);
        if (property_result.HasError()) {
          spdlog::debug("Encountered an error while trying to get edge properties. Transaction id: {}",
                        req.transaction_id.logical_id);
          return false;
        }
        value_properties.emplace_back(FromPropertyValueToValue(std::move(property_result.GetValue())));
      }
      using EdgeWithSpecificProperties = msgs::ExpandOneResultRow::EdgeWithSpecificProperties;
      EdgeWithSpecificProperties edges{ToMsgsVertexId(edge.FromVertex()), msgs::EdgeType{edge.EdgeType()},
                                       edge.Gid().AsUint(), std::move(value_properties)};
      if (is_in_edge) {
        result_row.in_edges_with_specific_properties.push_back(std::move(edges));
      } else {
        result_row.out_edges_with_specific_properties.push_back(std::move(edges));
      }
      return true;
    };
  }

  /// Fill up source vertex
  const auto primary_key = ConvertPropertyVector(std::move(src_vertex.second));
  auto v_acc = acc.FindVertex(primary_key, View::NEW);

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

  msgs::ExpandOneResultRow result_row;
  result_row.src_vertex = std::move(*source_vertex);
  result_row.src_vertex_properties = std::move(*src_vertex_properties);
  static constexpr bool kInEdges = true;
  static constexpr bool kOutEdges = false;
  if (!in_edges.empty() && !FillEdges<kInEdges>(in_edges, req, result_row, edge_filler)) {
    return std::nullopt;
  }
  if (!out_edges.empty() && !FillEdges<kOutEdges>(out_edges, req, result_row, edge_filler)) {
    return std::nullopt;
  }

  return result_row;
}
};  // namespace
msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

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
    std::vector<LabelId> converted_label_ids;
    converted_label_ids.reserve(new_vertex.label_ids.size());

    std::transform(new_vertex.label_ids.begin(), new_vertex.label_ids.end(), std::back_inserter(converted_label_ids),
                   [](const auto &label_id) { return label_id.id; });

    // TODO(jbajic) sending primary key as vector breaks validation on storage side
    // cannot map id -> value
    PrimaryKey transformed_pk;
    std::transform(new_vertex.primary_key.begin(), new_vertex.primary_key.end(), std::back_inserter(transformed_pk),
                   [](const auto &val) { return ToPropertyValue(val); });
    auto result_schema = acc.CreateVertexAndValidate(converted_label_ids, transformed_pk, converted_property_map);

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

  return msgs::CreateVerticesResponse{.success = action_successful};
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

  return msgs::UpdateVerticesResponse{.success = action_successful};
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
        case msgs::DeleteVerticesRequest::DeletionType::DELETE: {
          auto result = acc.DeleteVertex(&vertex_acc.value());
          if (result.HasError() || !(result.GetValue().has_value())) {
            action_successful = false;
            spdlog::debug("Error while trying to delete vertex. Transaction id: {}", req.transaction_id.logical_id);
          }

          break;
        }
        case msgs::DeleteVerticesRequest::DeletionType::DETACH_DELETE: {
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

  return msgs::DeleteVerticesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateExpandRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  bool action_successful = true;

  for (auto &new_expand : req.new_expands) {
    const auto from_vertex_id =
        v3::VertexId{new_expand.src_vertex.first.id, ConvertPropertyVector(std::move(new_expand.src_vertex.second))};

    const auto to_vertex_id =
        VertexId{new_expand.dest_vertex.first.id, ConvertPropertyVector(std::move(new_expand.dest_vertex.second))};

    if (!(shard_->IsVertexBelongToShard(from_vertex_id) || shard_->IsVertexBelongToShard(to_vertex_id))) {
      action_successful = false;
      spdlog::debug("Error while trying to insert edge, none of the vertices belong to this shard. Transaction id: {}",
                    req.transaction_id.logical_id);
      break;
    }

    auto edge_acc = acc.CreateEdge(from_vertex_id, to_vertex_id, new_expand.type.id, Gid::FromUint(new_expand.id.gid));
    if (edge_acc.HasValue()) {
      auto edge = edge_acc.GetValue();
      if (!new_expand.properties.empty()) {
        for (const auto &[property, value] : new_expand.properties) {
          if (const auto maybe_error = edge.SetProperty(property, ToPropertyValue(value)); maybe_error.HasError()) {
            action_successful = false;
            spdlog::debug("Setting edge property was not successful. Transaction id: {}",
                          req.transaction_id.logical_id);
            break;
          }
          if (!action_successful) {
            break;
          }
        }
      }
    } else {
      action_successful = false;
      spdlog::debug("Creating edge was not successful. Transaction id: {}", req.transaction_id.logical_id);
      break;
    }

    // Add properties to the edge if there is any
    if (!new_expand.properties.empty()) {
      for (auto &[edge_prop_key, edge_prop_val] : new_expand.properties) {
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

  return msgs::CreateExpandResponse{.success = action_successful};
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

  return msgs::DeleteEdgesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateEdgesRequest &&req) {
  // TODO(antaljanosbenjamin): handle when the vertex is the destination vertex
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

  return msgs::UpdateEdgesResponse{.success = action_successful};
}

msgs::ReadResponses ShardRsm::HandleRead(msgs::ScanVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  bool action_successful = true;

  std::vector<msgs::ScanResultRow> results;
  std::optional<msgs::VertexId> next_start_id;

  const auto view = View(req.storage_view);
  auto vertex_iterable = acc.Vertices(view);
  bool did_reach_starting_point = false;
  uint64_t sample_counter = 0;

  const auto start_ids = ConvertPropertyVector(std::move(req.start_id.second));
  auto dba = DbAccessor{&acc};

  for (auto it = vertex_iterable.begin(); it != vertex_iterable.end(); ++it) {
    const auto &vertex = *it;

    if (start_ids <= vertex.PrimaryKey(View(req.storage_view)).GetValue()) {
      did_reach_starting_point = true;
    }

    if (did_reach_starting_point) {
      std::vector<Value> expression_results;

      // TODO(gvolfing) it should be enough to check these only once.
      if (vertex.Properties(View(req.storage_view)).HasError()) {
        action_successful = false;
        spdlog::debug("Could not retrive properties from VertexAccessor. Transaction id: {}",
                      req.transaction_id.logical_id);
        break;
      }
      if (!req.filter_expressions.empty()) {
        // NOTE - DbAccessor might get removed in the future.
        const bool eval = FilterOnVertex(dba, vertex, req.filter_expressions, expr::identifier_node_symbol);
        if (!eval) {
          continue;
        }
      }
      if (!req.vertex_expressions.empty()) {
        expression_results = ConvertToValueVectorFromTypedValueVector(
            EvaluateVertexExpressions(dba, vertex, req.vertex_expressions, expr::identifier_node_symbol));
      }

      std::optional<std::map<PropertyId, Value>> found_props;

      const auto *schema = shard_->GetSchema(shard_->PrimaryLabel());
      if (req.props_to_return) {
        found_props = CollectSpecificPropertiesFromAccessor(vertex, req.props_to_return.value(), view);
      } else {
        found_props = CollectAllPropertiesFromAccessor(vertex, view, schema);
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
      if (req.batch_limit && sample_counter == req.batch_limit) {
        // Reached the maximum specified batch size.
        // Get the next element before exiting.
        const auto &next_vertex = *(++it);
        next_start_id = ConstructValueVertex(next_vertex, view).vertex_v.id;

        break;
      }
    }
  }

  msgs::ScanVerticesResponse resp{};
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

  std::vector<msgs::ExpandOneResultRow> results;

  for (auto &src_vertex : req.src_vertices) {
    auto result = GetExpandOneResult(acc, src_vertex, req);

    if (!result) {
      action_successful = false;
      break;
    }

    results.emplace_back(result.value());
  }

  msgs::ExpandOneResponse resp{};
  resp.success = action_successful;
  if (action_successful) {
    resp.result = std::move(results);
  }

  return resp;
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CommitRequest &&req) {
  shard_->Access(req.transaction_id).Commit(req.commit_timestamp);
  return msgs::CommitResponse{true};
};

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
msgs::ReadResponses ShardRsm::HandleRead(msgs::GetPropertiesRequest && /*req*/) {
  return msgs::GetPropertiesResponse{};
}

}  //    namespace memgraph::storage::v3
