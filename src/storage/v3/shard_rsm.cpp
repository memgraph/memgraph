// Copyright 2023 Memgraph Ltd.
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
#include <exception>
#include <experimental/source_location>
#include <functional>
#include <iterator>
#include <optional>
#include <unordered_set>
#include <utility>
#include <variant>

#include "common/errors.hpp"
#include "parser/opencypher/parser.hpp"
#include "query/v2/requests.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/view.hpp"
#include "storage/v3/bindings/ast/ast.hpp"
#include "storage/v3/bindings/cypher_main_visitor.hpp"
#include "storage/v3/bindings/db_accessor.hpp"
#include "storage/v3/bindings/eval.hpp"
#include "storage/v3/bindings/frame.hpp"
#include "storage/v3/bindings/pretty_print_ast_to_original_expression.hpp"
#include "storage/v3/bindings/symbol_generator.hpp"
#include "storage/v3/bindings/symbol_table.hpp"
#include "storage/v3/bindings/typed_value.hpp"
#include "storage/v3/conversions.hpp"
#include "storage/v3/expr.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/request_helper.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/value_conversions.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage/v3/vertex_id.hpp"
#include "storage/v3/view.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage::v3 {
using msgs::Label;
using msgs::PropertyId;
using msgs::Value;

using conversions::ConvertPropertyMap;
using conversions::ConvertPropertyVector;
using conversions::ConvertValueVector;
using conversions::FromMap;
using conversions::FromPropertyValueToValue;
using conversions::ToMsgsVertexId;
using conversions::ToPropertyValue;

auto CreateErrorResponse(const ShardError &shard_error, const auto transaction_id, const std::string_view action) {
  msgs::ShardError message_shard_error{shard_error.code, shard_error.message};
  spdlog::debug("{} In transaction {} {} failed: {}: {}", shard_error.source, transaction_id.logical_id, action,
                ErrorCodeToString(shard_error.code), shard_error.message);
  return message_shard_error;
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  std::optional<msgs::ShardError> shard_error;

  for (auto &new_vertex : req.new_vertices) {
    /// TODO(gvolfing) Consider other methods than converting. Change either
    /// the way that the property map is stored in the messages, or the
    /// signature of CreateVertexAndValidate.
    auto converted_property_map = ConvertPropertyMap(new_vertex.properties);

    // TODO(gvolfing) make sure if this conversion is actually needed.
    std::vector<LabelId> converted_label_ids;
    converted_label_ids.reserve(new_vertex.label_ids.size());

    std::transform(new_vertex.label_ids.begin(), new_vertex.label_ids.end(), std::back_inserter(converted_label_ids),
                   [](const auto &label_id) { return label_id.id; });

    PrimaryKey transformed_pk;
    std::transform(new_vertex.primary_key.begin(), new_vertex.primary_key.end(), std::back_inserter(transformed_pk),
                   [](msgs::Value &val) { return ToPropertyValue(std::move(val)); });
    auto result_schema = acc.CreateVertexAndValidate(converted_label_ids, transformed_pk, converted_property_map);

    if (result_schema.HasError()) {
      shard_error.emplace(CreateErrorResponse(result_schema.GetError(), req.transaction_id, "creating vertices"));
      break;
    }
  }

  return msgs::CreateVerticesResponse{std::move(shard_error)};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  std::optional<msgs::ShardError> shard_error;
  for (auto &vertex : req.update_vertices) {
    auto vertex_to_update = acc.FindVertex(ConvertPropertyVector(std::move(vertex.primary_key)), View::OLD);
    if (!vertex_to_update) {
      shard_error.emplace(msgs::ShardError{common::ErrorCode::OBJECT_NOT_FOUND});
      spdlog::debug("In transaction {} vertex could not be found while trying to update its properties.",
                    req.transaction_id.logical_id);
      break;
    }

    for (const auto label : vertex.add_labels) {
      if (const auto maybe_error = vertex_to_update->AddLabelAndValidate(label); maybe_error.HasError()) {
        shard_error.emplace(CreateErrorResponse(maybe_error.GetError(), req.transaction_id, "adding label"));
        break;
      }
    }
    for (const auto label : vertex.remove_labels) {
      if (const auto maybe_error = vertex_to_update->RemoveLabelAndValidate(label); maybe_error.HasError()) {
        shard_error.emplace(CreateErrorResponse(maybe_error.GetError(), req.transaction_id, "adding label"));
        break;
      }
    }

    for (auto &update_prop : vertex.property_updates) {
      if (const auto result_schema = vertex_to_update->SetPropertyAndValidate(
              update_prop.first, ToPropertyValue(std::move(update_prop.second)));
          result_schema.HasError()) {
        shard_error.emplace(CreateErrorResponse(result_schema.GetError(), req.transaction_id, "adding label"));
        break;
      }
    }
  }

  return msgs::UpdateVerticesResponse{std::move(shard_error)};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::DeleteVerticesRequest &&req) {
  std::optional<msgs::ShardError> shard_error;
  auto acc = shard_->Access(req.transaction_id);

  for (auto &propval : req.primary_keys) {
    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(std::move(propval)), View::OLD);

    if (!vertex_acc) {
      shard_error.emplace(msgs::ShardError{common::ErrorCode::OBJECT_NOT_FOUND});
      spdlog::debug("In transaction {} vertex could not be found while trying to delete it.",
                    req.transaction_id.logical_id);
      break;
    }
    // TODO(gvolfing)
    // Since we will not have different kinds of deletion types in one transaction,
    // we dont have to enter the switch statement on every iteration. Optimize this.
    switch (req.deletion_type) {
      case msgs::DeleteVerticesRequest::DeletionType::DELETE: {
        auto result = acc.DeleteVertex(&vertex_acc.value());
        if (result.HasError() || !(result.GetValue().has_value())) {
          shard_error.emplace(CreateErrorResponse(result.GetError(), req.transaction_id, "deleting vertices"));
        }
        break;
      }
      case msgs::DeleteVerticesRequest::DeletionType::DETACH_DELETE: {
        auto result = acc.DetachDeleteVertex(&vertex_acc.value());
        if (result.HasError() || !(result.GetValue().has_value())) {
          shard_error.emplace(CreateErrorResponse(result.GetError(), req.transaction_id, "deleting vertices"));
        }
        break;
      }
    }
    if (shard_error) {
      break;
    }
  }

  return msgs::DeleteVerticesResponse{std::move(shard_error)};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateExpandRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  std::optional<msgs::ShardError> shard_error;

  for (auto &new_expand : req.new_expands) {
    const auto from_vertex_id =
        v3::VertexId{new_expand.src_vertex.first.id, ConvertPropertyVector(std::move(new_expand.src_vertex.second))};

    const auto to_vertex_id =
        VertexId{new_expand.dest_vertex.first.id, ConvertPropertyVector(std::move(new_expand.dest_vertex.second))};

    if (!(shard_->IsVertexBelongToShard(from_vertex_id) || shard_->IsVertexBelongToShard(to_vertex_id))) {
      shard_error = msgs::ShardError{common::ErrorCode::OBJECT_NOT_FOUND,
                                     "Error while trying to insert edge, none of the vertices belong to this shard"};
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
            shard_error.emplace(
                CreateErrorResponse(maybe_error.GetError(), req.transaction_id, "setting edge property"));
            break;
          }
        }
        if (shard_error) {
          break;
        }
      }
    } else {
      // TODO Code for this
      shard_error = msgs::ShardError{common::ErrorCode::OBJECT_NOT_FOUND};
      spdlog::debug("Creating edge was not successful. Transaction id: {}", req.transaction_id.logical_id);
      break;
    }

    // Add properties to the edge if there is any
    if (!new_expand.properties.empty()) {
      for (auto &[edge_prop_key, edge_prop_val] : new_expand.properties) {
        auto set_result = edge_acc->SetProperty(edge_prop_key, ToPropertyValue(std::move(edge_prop_val)));
        if (set_result.HasError()) {
          shard_error.emplace(CreateErrorResponse(set_result.GetError(), req.transaction_id, "adding edge property"));
          break;
        }
      }
    }
  }

  return msgs::CreateExpandResponse{std::move(shard_error)};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::DeleteEdgesRequest &&req) {
  std::optional<msgs::ShardError> shard_error;
  auto acc = shard_->Access(req.transaction_id);

  for (auto &edge : req.edges) {
    if (shard_error) {
      break;
    }

    auto edge_acc = acc.DeleteEdge(VertexId(edge.src.first.id, ConvertPropertyVector(std::move(edge.src.second))),
                                   VertexId(edge.dst.first.id, ConvertPropertyVector(std::move(edge.dst.second))),
                                   Gid::FromUint(edge.id.gid));
    if (edge_acc.HasError() || !edge_acc.HasValue()) {
      shard_error.emplace(CreateErrorResponse(edge_acc.GetError(), req.transaction_id, "delete edge"));
      continue;
    }
  }

  return msgs::DeleteEdgesResponse{std::move(shard_error)};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateEdgesRequest &&req) {
  // TODO(antaljanosbenjamin): handle when the vertex is the destination vertex
  auto acc = shard_->Access(req.transaction_id);

  std::optional<msgs::ShardError> shard_error;

  for (auto &edge : req.new_properties) {
    if (shard_error) {
      break;
    }

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(std::move(edge.src.second)), View::OLD);
    if (!vertex_acc) {
      shard_error = msgs::ShardError{common::ErrorCode::OBJECT_NOT_FOUND, "Source vertex was not found"};
      spdlog::debug("Encountered an error while trying to acquire VertexAccessor with transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }

    // Since we are using the source vertex of the edge we are only interested
    // in the vertex's out-going edges
    auto edges_res = vertex_acc->OutEdges(View::OLD);
    if (edges_res.HasError()) {
      shard_error.emplace(CreateErrorResponse(edges_res.GetError(), req.transaction_id, "update edge"));
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
            // TODO(jbajic) why not set action unsuccessful here?
            shard_error.emplace(CreateErrorResponse(edges_res.GetError(), req.transaction_id, "update edge"));
          }
        }
      }
    }

    if (!edge_accessor_did_match) {
      // TODO(jbajic) Do we need this
      shard_error = msgs::ShardError{common::ErrorCode::OBJECT_NOT_FOUND, "Edge was not found"};
      spdlog::debug("Could not find the Edge with the specified Gid. Transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }
  }

  return msgs::UpdateEdgesResponse{std::move(shard_error)};
}

msgs::ReadResponses ShardRsm::HandleRead(msgs::ScanVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  std::optional<msgs::ShardError> shard_error;

  std::vector<msgs::ScanResultRow> results;
  if (req.batch_limit) {
    results.reserve(*req.batch_limit);
  }
  std::optional<msgs::VertexId> next_start_id;

  const auto view = View(req.storage_view);
  auto dba = DbAccessor{&acc};
  const auto emplace_scan_result = [&](const VertexAccessor &vertex) {
    std::vector<Value> expression_results;
    if (!req.filter_expressions.empty()) {
      // NOTE - DbAccessor might get removed in the future.
      const bool eval = FilterOnVertex(dba, vertex, req.filter_expressions);
      if (!eval) {
        return;
      }
    }
    if (!req.vertex_expressions.empty()) {
      // NOTE - DbAccessor might get removed in the future.
      expression_results = ConvertToValueVectorFromTypedValueVector(
          EvaluateVertexExpressions(dba, vertex, req.vertex_expressions, expr::identifier_node_symbol));
    }

    auto found_props = std::invoke([&]() {
      if (req.props_to_return) {
        return CollectSpecificPropertiesFromAccessor(vertex, req.props_to_return.value(), view);
      }
      const auto *schema = shard_->GetSchema(shard_->PrimaryLabel());
      MG_ASSERT(schema);
      return CollectAllPropertiesFromAccessor(vertex, view, *schema);
    });

    // TODO(gvolfing) -VERIFY-
    // Vertex is separated from the properties in the response.
    // Is it useful to return just a vertex without the properties?
    if (found_props.HasError()) {
      shard_error = msgs::ShardError{common::ErrorCode::OBJECT_NOT_FOUND, "Requested properties were not found!"};
    }

    results.emplace_back(msgs::ScanResultRow{.vertex = ConstructValueVertex(vertex, view).vertex_v,
                                             .props = FromMap(found_props.GetValue()),
                                             .evaluated_vertex_expressions = std::move(expression_results)});
  };

  const auto start_id = ConvertPropertyVector(std::move(req.start_id.second));
  uint64_t sample_counter{0};
  auto vertex_iterable = acc.Vertices(view);
  if (!req.order_bys.empty()) {
    const auto ordered = OrderByVertices(dba, vertex_iterable, req.order_bys);
    // we are traversing Elements
    auto it = GetStartOrderedElementsIterator(ordered, start_id, View(req.storage_view));
    for (; it != ordered.end(); ++it) {
      emplace_scan_result(it->object_acc);
      ++sample_counter;
      if (req.batch_limit && sample_counter == req.batch_limit) {
        // Reached the maximum specified batch size.
        // Get the next element before exiting.
        ++it;
        if (it != ordered.end()) {
          const auto &next_vertex = it->object_acc;
          next_start_id = ConstructValueVertex(next_vertex, view).vertex_v.id;
        }

        break;
      }
    }
  } else {
    // We are going through VerticesIterable::Iterator
    auto it = GetStartVertexIterator(vertex_iterable, start_id, View(req.storage_view));
    for (; it != vertex_iterable.end(); ++it) {
      emplace_scan_result(*it);

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

  msgs::ScanVerticesResponse resp{.error = std::move(shard_error)};
  if (!resp.error) {
    resp.next_start_id = next_start_id;
    resp.results = std::move(results);
  }

  return resp;
}

msgs::ReadResponses ShardRsm::HandleRead(msgs::ExpandOneRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  std::optional<msgs::ShardError> shard_error;

  std::vector<msgs::ExpandOneResultRow> results;
  const auto batch_limit = req.limit;
  auto dba = DbAccessor{&acc};

  auto maybe_filter_based_on_edge_uniqueness = InitializeEdgeUniquenessFunction(req.only_unique_neighbor_rows);
  auto edge_filler = InitializeEdgeFillerFunction(req);

  std::vector<VertexAccessor> vertex_accessors;
  vertex_accessors.reserve(req.src_vertices.size());
  for (auto &src_vertex : req.src_vertices) {
    // Get Vertex acc
    auto src_vertex_acc_opt = acc.FindVertex(ConvertPropertyVector((src_vertex.second)), View::NEW);
    if (!src_vertex_acc_opt) {
      shard_error = msgs::ShardError{common::ErrorCode::OBJECT_NOT_FOUND, "Source vertex was not found."};
      spdlog::debug("Encountered an error while trying to obtain VertexAccessor. Transaction id: {}",
                    req.transaction_id.logical_id);
      break;
    }
    if (!req.filters.empty()) {
      // NOTE - DbAccessor might get removed in the future.
      const bool eval = FilterOnVertex(dba, src_vertex_acc_opt.value(), req.filters);
      if (!eval) {
        continue;
      }
    }

    vertex_accessors.emplace_back(src_vertex_acc_opt.value());
  }

  if (!req.order_by_vertices.empty()) {
    // Can we do differently to avoid this? We need OrderByElements but currently it returns vector<Element>, so this
    // workaround is here to avoid more duplication later
    auto local_sorted_vertices = OrderByVertices(dba, vertex_accessors, req.order_by_vertices);
    vertex_accessors.clear();
    std::transform(local_sorted_vertices.begin(), local_sorted_vertices.end(), std::back_inserter(vertex_accessors),
                   [](auto &vertex) { return vertex.object_acc; });
  }

  for (const auto &src_vertex_acc : vertex_accessors) {
    auto label_id = src_vertex_acc.PrimaryLabel(View::NEW);
    if (label_id.HasError()) {
      shard_error.emplace(CreateErrorResponse(label_id.GetError(), req.transaction_id, "getting label"));
    }

    auto primary_key = src_vertex_acc.PrimaryKey(View::NEW);
    if (primary_key.HasError()) {
      shard_error.emplace(CreateErrorResponse(primary_key.GetError(), req.transaction_id, "getting primary key"));
      break;
    }

    msgs::VertexId src_vertex(msgs::Label{.id = *label_id}, conversions::ConvertValueVector(*primary_key));

    auto maybe_result = std::invoke([&]() {
      if (req.order_by_edges.empty()) {
        const auto *schema = shard_->GetSchema(shard_->PrimaryLabel());
        MG_ASSERT(schema);
        return GetExpandOneResult(acc, src_vertex, req, maybe_filter_based_on_edge_uniqueness, edge_filler, *schema);
      }
      auto [in_edge_accessors, out_edge_accessors] = GetEdgesFromVertex(src_vertex_acc, req.direction);
      const auto in_ordered_edges = OrderByEdges(dba, in_edge_accessors, req.order_by_edges, src_vertex_acc);
      const auto out_ordered_edges = OrderByEdges(dba, out_edge_accessors, req.order_by_edges, src_vertex_acc);

      std::vector<EdgeAccessor> in_edge_ordered_accessors;
      std::transform(in_ordered_edges.begin(), in_ordered_edges.end(), std::back_inserter(in_edge_ordered_accessors),
                     [](const auto &edge_element) { return edge_element.object_acc; });

      std::vector<EdgeAccessor> out_edge_ordered_accessors;
      std::transform(out_ordered_edges.begin(), out_ordered_edges.end(), std::back_inserter(out_edge_ordered_accessors),
                     [](const auto &edge_element) { return edge_element.object_acc; });
      const auto *schema = shard_->GetSchema(shard_->PrimaryLabel());
      MG_ASSERT(schema);
      return GetExpandOneResult(src_vertex_acc, src_vertex, req, in_edge_ordered_accessors, out_edge_ordered_accessors,
                                maybe_filter_based_on_edge_uniqueness, edge_filler, *schema);
    });

    if (maybe_result.HasError()) {
      shard_error.emplace(CreateErrorResponse(primary_key.GetError(), req.transaction_id, "getting primary key"));
      break;
    }

    results.emplace_back(std::move(maybe_result.GetValue()));
    if (batch_limit.has_value() && results.size() >= batch_limit.value()) {
      break;
    }
  }

  msgs::ExpandOneResponse resp{.error = std::move(shard_error)};
  if (!resp.error) {
    resp.result = std::move(results);
  }

  return resp;
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CommitRequest &&req) {
  shard_->Access(req.transaction_id).Commit(req.commit_timestamp);
  return msgs::CommitResponse{};
};

msgs::ReadResponses ShardRsm::HandleRead(msgs::GetPropertiesRequest &&req) {
  if (!req.vertex_ids.empty() && !req.vertices_and_edges.empty()) {
    auto shard_error = SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
    auto error = CreateErrorResponse(shard_error, req.transaction_id, "");
    return msgs::GetPropertiesResponse{.error = {}};
  }

  auto shard_acc = shard_->Access(req.transaction_id);
  auto dba = DbAccessor{&shard_acc};
  const auto view = storage::v3::View::NEW;

  auto transform_props = [](std::map<PropertyId, Value> &&value) {
    std::vector<std::pair<PropertyId, Value>> result;
    result.reserve(value.size());
    for (auto &[id, val] : value) {
      result.emplace_back(std::make_pair(id, std::move(val)));
    }
    return result;
  };

  auto collect_props = [this, &req](
                           const VertexAccessor &v_acc,
                           const std::optional<EdgeAccessor> &e_acc) -> ShardResult<std::map<PropertyId, Value>> {
    if (!req.property_ids) {
      const auto *schema = shard_->GetSchema(shard_->PrimaryLabel());
      MG_ASSERT(schema);

      if (e_acc) {
        return CollectAllPropertiesFromAccessor(*e_acc, view);
      }
      return CollectAllPropertiesFromAccessor(v_acc, view, *schema);
    }

    if (e_acc) {
      return CollectSpecificPropertiesFromAccessor(*e_acc, *req.property_ids, view);
    }
    return CollectSpecificPropertiesFromAccessor(v_acc, *req.property_ids, view);
  };

  auto find_edge = [](const VertexAccessor &v, msgs::EdgeId e) -> std::optional<EdgeAccessor> {
    auto in = v.InEdges(view);
    MG_ASSERT(in.HasValue());
    for (auto &edge : in.GetValue()) {
      if (edge.Gid().AsUint() == e.gid) {
        return edge;
      }
    }
    auto out = v.OutEdges(view);
    MG_ASSERT(out.HasValue());
    for (auto &edge : out.GetValue()) {
      if (edge.Gid().AsUint() == e.gid) {
        return edge;
      }
    }
    return std::nullopt;
  };

  const auto has_expr_to_evaluate = !req.expressions.empty();
  auto emplace_result_row =
      [dba, transform_props, collect_props, has_expr_to_evaluate, &req](
          const VertexAccessor &v_acc,
          const std::optional<EdgeAccessor> e_acc) mutable -> ShardResult<msgs::GetPropertiesResultRow> {
    auto maybe_id = v_acc.Id(view);
    if (maybe_id.HasError()) {
      return {maybe_id.GetError()};
    }
    const auto &id = maybe_id.GetValue();
    std::optional<msgs::EdgeId> e_id;
    if (e_acc) {
      e_id = msgs::EdgeId{e_acc->Gid().AsUint()};
    }
    msgs::VertexId v_id{msgs::Label{id.primary_label}, ConvertValueVector(id.primary_key)};
    auto maybe_props = collect_props(v_acc, e_acc);
    if (maybe_props.HasError()) {
      return {maybe_props.GetError()};
    }
    auto props = transform_props(std::move(maybe_props.GetValue()));
    auto result = msgs::GetPropertiesResultRow{.vertex = std::move(v_id), .edge = e_id, .props = std::move(props)};
    if (has_expr_to_evaluate) {
      std::vector<Value> e_results;
      if (e_acc) {
        e_results =
            ConvertToValueVectorFromTypedValueVector(EvaluateEdgeExpressions(dba, v_acc, *e_acc, req.expressions));
      } else {
        e_results = ConvertToValueVectorFromTypedValueVector(
            EvaluateVertexExpressions(dba, v_acc, req.expressions, expr::identifier_node_symbol));
      }
      result.evaluated_expressions = std::move(e_results);
    }
    return {std::move(result)};
  };

  auto get_limit = [&req](const auto &elements) {
    size_t limit = elements.size();
    if (req.limit && *req.limit < elements.size()) {
      limit = *req.limit;
    }
    return limit;
  };

  auto collect_response = [get_limit, &req](auto &elements, auto create_result_row) -> msgs::ReadResponses {
    msgs::GetPropertiesResponse response;
    const auto limit = get_limit(elements);
    for (size_t index = 0; index != limit; ++index) {
      auto result_row = create_result_row(elements[index]);
      if (result_row.HasError()) {
        return msgs::GetPropertiesResponse{.error = CreateErrorResponse(result_row.GetError(), req.transaction_id, "")};
      }
      response.result_row.push_back(std::move(result_row.GetValue()));
    }
    return response;
  };

  std::vector<VertexAccessor> vertices;
  std::vector<EdgeAccessor> edges;

  auto parse_and_filter = [dba, &vertices](auto &container, auto projection, auto filter, auto maybe_get_edge) mutable {
    for (const auto &elem : container) {
      const auto &[label, pk_v] = projection(elem);
      auto pk = ConvertPropertyVector(pk_v);
      auto v_acc = dba.FindVertex(pk, view);
      if (!v_acc || filter(*v_acc, maybe_get_edge(elem))) {
        continue;
      }
      vertices.push_back(*v_acc);
    }
  };
  auto identity = [](auto &elem) { return elem; };

  auto filter_vertex = [dba, req](const auto &acc, const auto & /*edge*/) mutable {
    if (!req.filter) {
      return false;
    }
    return !FilterOnVertex(dba, acc, {*req.filter});
  };

  auto filter_edge = [dba, &edges, &req, find_edge](const auto &acc, const auto &edge) mutable {
    auto e_acc = find_edge(acc, edge);
    if (!e_acc) {
      return true;
    }

    if (req.filter && !FilterOnEdge(dba, acc, *e_acc, {*req.filter})) {
      return true;
    }
    edges.push_back(*e_acc);
    return false;
  };

  // Handler logic here
  if (!req.vertex_ids.empty()) {
    parse_and_filter(req.vertex_ids, identity, filter_vertex, identity);
  } else {
    parse_and_filter(
        req.vertices_and_edges, [](auto &e) { return e.first; }, filter_edge, [](auto &e) { return e.second; });
  }

  if (!req.vertex_ids.empty()) {
    if (!req.order_by.empty()) {
      auto elements = OrderByVertices(dba, vertices, req.order_by);
      return collect_response(elements, [emplace_result_row](auto &element) mutable {
        return emplace_result_row(element.object_acc, std::nullopt);
      });
    }
    return collect_response(vertices,
                            [emplace_result_row](auto &acc) mutable { return emplace_result_row(acc, std::nullopt); });
  }

  if (!req.order_by.empty()) {
    auto elements = OrderByEdges(dba, edges, req.order_by, vertices);
    return collect_response(elements, [emplace_result_row](auto &element) mutable {
      return emplace_result_row(element.object_acc.first, element.object_acc.second);
    });
  }

  struct ZipView {
    ZipView(std::vector<VertexAccessor> &v, std::vector<EdgeAccessor> &e) : v(v), e(e) {}
    size_t size() const { return v.size(); }
    auto operator[](size_t index) { return std::make_pair(v[index], e[index]); }

   private:
    std::vector<VertexAccessor> &v;
    std::vector<EdgeAccessor> &e;
  };

  ZipView vertices_and_edges(vertices, edges);
  return collect_response(vertices_and_edges, [emplace_result_row](const auto &acc) mutable {
    return emplace_result_row(acc.first, acc.second);
  });
}

}  // namespace memgraph::storage::v3
