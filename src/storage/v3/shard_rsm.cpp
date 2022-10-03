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
using memgraph::msgs::VertexId;

using memgraph::storage::conversions::ConvertPropertyVector;
using memgraph::storage::conversions::ConvertValueVector;
using memgraph::storage::conversions::ToPropertyValue;
using memgraph::storage::conversions::ToValue;

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
    memgraph::storage::v3::View view) {
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
    ret.emplace(std::make_pair(prop, ToValue(value)));
  }

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(
    const memgraph::storage::v3::VertexAccessor &acc, memgraph::storage::v3::View view) {
  std::map<PropertyId, Value> ret;
  auto iter = acc.Properties(view);
  if (iter.HasError()) {
    spdlog::debug("Encountered an error while trying to get vertex properties.");
  }

  for (const auto &[prop_key, prop_val] : iter.GetValue()) {
    ret.emplace(prop_key, ToValue(prop_val));
  }

  return ret;
}

Value ConstructValueVertex(const memgraph::storage::v3::VertexAccessor &acc, memgraph::storage::v3::View view) {
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

    // TBD -> we need to specify temporal types, not a priority.
    case TypedValue::Type::Date:
    case TypedValue::Type::LocalTime:
    case TypedValue::Type::LocalDateTime:
    case TypedValue::Type::Duration:

    // TODO(gvolfing) -VERIFY- do we need these?
    case TypedValue::Type::Vertex:
    case TypedValue::Type::Edge:
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

/////////////////////////////////////////////////////////////////////////////////

std::vector<PropertyId> NamesToProperties(const std::vector<std::string> &property_names,
                                          memgraph::expr::DbAccessor &dba) {
  std::vector<PropertyId> properties;
  properties.reserve(property_names.size());
  for (const auto &name : property_names) {
    properties.push_back(dba.NameToProperty(name));
  }
  return properties;
}

// TODO(gvolfing) -VERIFY-
// is this the good namespace for LabelId?
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
  const auto properties = v_acc.Properties(memgraph::storage::v3::View::OLD);
  std::vector<PropertyId> properties_in_shard;

  for (const auto &prop : properties.GetValue()) {
    properties_in_shard.emplace_back(prop.first);
  }

  ctx.properties = std::move(properties_in_shard);

  // Fill EvaluationContext up with labels
  const auto labels = v_acc.Labels(memgraph::storage::v3::View::OLD);
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

  return msgs::CreateVerticesResponse{action_successful};
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

  return msgs::DeleteVerticesResponse{action_successful};
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
  }

  return msgs::CreateEdgesResponse{action_successful};
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

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CommitRequest &&req) {
  shard_->Access(req.transaction_id).Commit(req.commit_timestamp);
  return msgs::CommitResponse{true};
};

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateVerticesRequest && /*req*/) {
  return msgs::UpdateVerticesResponse{};
}
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
msgs::WriteResponses ShardRsm::ApplyWrite(msgs::DeleteEdgesRequest && /*req*/) { return msgs::DeleteEdgesResponse{}; }
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateEdgesRequest && /*req*/) { return msgs::UpdateEdgesResponse{}; }
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
msgs::ReadResponses ShardRsm::HandleRead(msgs::ExpandOneRequest && /*req*/) { return msgs::ExpandOneResponse{}; }
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
msgs::ReadResponses ShardRsm::HandleRead(msgs::GetPropertiesRequest && /*req*/) {
  return msgs::GetPropertiesResponse{};
}

}  //    namespace memgraph::storage::v3
