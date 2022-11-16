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
#include <optional>
#include <unordered_set>
#include <utility>

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
#include "storage/v3/expr.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/request_helper.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/storage.hpp"
#include "storage/v3/value_conversions.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage/v3/vertex_id.hpp"
#include "storage/v3/view.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage::v3 {
using msgs::Label;  // #NoCommit not needed?
using msgs::PropertyId;
using msgs::Value;

using conversions::ConvertPropertyMap;
using conversions::ConvertPropertyVector;
using conversions::ConvertValueVector;
using conversions::FromMap;
using conversions::FromPropertyValueToValue;
using conversions::ToMsgsVertexId;
using conversions::ToPropertyValue;

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  bool action_successful = true;

  for (auto &new_vertex : req.new_vertices) {
    /// TODO(gvolfing) Consider other methods than converting. Change either
    /// the way that the property map is stored in the messages, or the
    /// signature of CreateVertexAndValidate.
    auto converted_property_map = ConvertPropertyMap(std::move(new_vertex.properties));

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
      auto &error = result_schema.GetError();

      std::visit(
          []<typename T>(T &&error) {
            using ErrorType = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<ErrorType, SchemaViolation>) {
              spdlog::debug("Creating vertex failed with error: SchemaViolation");
            } else if constexpr (std::is_same_v<ErrorType, Error>) {
              switch (error) {
                case Error::DELETED_OBJECT:
                  spdlog::debug("Creating vertex failed with error: DELETED_OBJECT");
                  break;
                case Error::NONEXISTENT_OBJECT:
                  spdlog::debug("Creating vertex failed with error: NONEXISTENT_OBJECT");
                  break;
                case Error::SERIALIZATION_ERROR:
                  spdlog::debug("Creating vertex failed with error: SERIALIZATION_ERROR");
                  break;
                case Error::PROPERTIES_DISABLED:
                  spdlog::debug("Creating vertex failed with error: PROPERTIES_DISABLED");
                  break;
                case Error::VERTEX_HAS_EDGES:
                  spdlog::debug("Creating vertex failed with error: VERTEX_HAS_EDGES");
                  break;
                case Error::VERTEX_ALREADY_INSERTED:
                  spdlog::debug("Creating vertex failed with error: VERTEX_ALREADY_INSERTED");
                  break;
              }
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
      auto result_schema =
          vertex_to_update->SetPropertyAndValidate(update_prop.first, ToPropertyValue(std::move(update_prop.second)));
      if (result_schema.HasError()) {
        auto &error = result_schema.GetError();

        std::visit(
            []<typename T>(T &&error) {
              using ErrorType = std::remove_cvref_t<T>;
              if constexpr (std::is_same_v<ErrorType, SchemaViolation>) {
                spdlog::debug("Updating vertex failed with error: SchemaViolation");
              } else if constexpr (std::is_same_v<ErrorType, Error>) {
                switch (error) {
                  case Error::DELETED_OBJECT:
                    spdlog::debug("Updating vertex failed with error: DELETED_OBJECT");
                    break;
                  case Error::NONEXISTENT_OBJECT:
                    spdlog::debug("Updating vertex failed with error: NONEXISTENT_OBJECT");
                    break;
                  case Error::SERIALIZATION_ERROR:
                    spdlog::debug("Updating vertex failed with error: SERIALIZATION_ERROR");
                    break;
                  case Error::PROPERTIES_DISABLED:
                    spdlog::debug("Updating vertex failed with error: PROPERTIES_DISABLED");
                    break;
                  case Error::VERTEX_HAS_EDGES:
                    spdlog::debug("Updating vertex failed with error: VERTEX_HAS_EDGES");
                    break;
                  case Error::VERTEX_ALREADY_INSERTED:
                    spdlog::debug("Updating vertex failed with error: VERTEX_ALREADY_INSERTED");
                    break;
                }
              } else {
                static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
              }
            },
            error);

        action_successful = false;

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
      const bool eval = FilterOnVertex(dba, vertex, req.filter_expressions, expr::identifier_node_symbol);
      if (!eval) {
        return;
      }
    }
    if (!req.vertex_expressions.empty()) {
      // NOTE - DbAccessor might get removed in the future.
      expression_results = ConvertToValueVectorFromTypedValueVector(
          EvaluateVertexExpressions(dba, vertex, req.vertex_expressions, expr::identifier_node_symbol));
    }

    std::optional<std::map<PropertyId, Value>> found_props;

    if (req.props_to_return) {
      found_props = CollectSpecificPropertiesFromAccessor(vertex, req.props_to_return.value(), view);
    } else {
      const auto *schema = shard_->GetSchema(shard_->PrimaryLabel());
      MG_ASSERT(schema);
      found_props = CollectAllPropertiesFromAccessor(vertex, view, *schema);
    }

    // TODO(gvolfing) -VERIFY-
    // Vertex is separated from the properties in the response.
    // Is it useful to return just a vertex without the properties?
    if (!found_props) {
      action_successful = false;
    }

    results.emplace_back(msgs::ScanResultRow{.vertex = ConstructValueVertex(vertex, view).vertex_v,
                                             .props = FromMap(found_props.value()),
                                             .evaluated_vertex_expressions = std::move(expression_results)});
  };

  const auto start_id = ConvertPropertyVector(std::move(req.start_id.second));
  uint64_t sample_counter{0};
  auto vertex_iterable = acc.Vertices(view);
  if (!req.order_bys.empty()) {
    const auto ordered = OrderByVertices(acc, dba, vertex_iterable, req.order_bys);
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
  auto batch_limit = req.limit;
  auto dba = DbAccessor{&acc};

  auto maybe_filter_based_on_edge_uniquness = InitializeEdgeUniqunessFunction(req.only_unique_neighbor_rows);
  auto edge_filler = InitializeEdgeFillerFunction(req);

  std::vector<VertexAccessor> vertex_accessors;
  vertex_accessors.reserve(req.src_vertices.size());
  for (auto &src_vertex : req.src_vertices) {
    // Get Vertex acc
    auto src_vertex_acc_opt = acc.FindVertex(ConvertPropertyVector((src_vertex.second)), View::NEW);
    if (!src_vertex_acc_opt) {
      action_successful = false;
      spdlog::debug("Encountered an error while trying to obtain VertexAccessor. Transaction id: {}",
                    req.transaction_id.logical_id);
      break;
    }
    if (!req.filters.empty()) {
      // NOTE - DbAccessor might get removed in the future.
      const bool eval = FilterOnVertex(dba, src_vertex_acc_opt.value(), req.filters, expr::identifier_node_symbol);
      if (!eval) {
        continue;
      }
    }

    vertex_accessors.emplace_back(src_vertex_acc_opt.value());
  }

  if (!req.order_by.empty()) {
    // Can we do differently to avoid this? We need OrderByElements but currently it returns vector<Element>, so this
    // workaround is here to avoid more duplication later
    auto local_sorted_vertices = OrderByVertices(
        acc, dba, vertex_accessors, req.order_by);  // #NoCommit see whether we can avoid the extra std::transform
    vertex_accessors.clear();
    std::transform(local_sorted_vertices.begin(), local_sorted_vertices.end(), std::back_inserter(vertex_accessors),
                   [](auto &vertex) { return vertex.object_acc; });
  }

  for (const auto &src_vertex_acc : vertex_accessors) {
    auto label_id = src_vertex_acc.PrimaryLabel(View::NEW);
    if (label_id.HasError()) {
      action_successful = false;
      break;
    }

    auto primary_key = src_vertex_acc.PrimaryKey(View::NEW);
    if (primary_key.HasError()) {
      action_successful = false;
      break;
    }

    msgs::VertexId src_vertice(msgs::Label{.id = *label_id}, conversions::ConvertValueVector(*primary_key));

    std::optional<msgs::ExpandOneResultRow> maybe_result;

    if (req.order_by.empty()) {
      auto schema = shard_->GetSchema(shard_->PrimaryLabel());
      MG_ASSERT(schema);
      maybe_result =
          GetExpandOneResult(acc, src_vertice, req, maybe_filter_based_on_edge_uniquness, edge_filler, *schema);

    } else {
      auto [in_edge_accessors, out_edge_accessors] = GetEdgesFromVertex(src_vertex_acc, req.direction);
      const auto in_ordered_edges = OrderByEdges(dba, in_edge_accessors, req.order_by, src_vertex_acc);
      const auto out_ordered_edges = OrderByEdges(dba, out_edge_accessors, req.order_by, src_vertex_acc);

      std::vector<EdgeAccessor> in_edge_ordered_accessors;
      std::transform(in_ordered_edges.begin(), in_ordered_edges.end(), std::back_inserter(in_edge_ordered_accessors),
                     [](const auto &edge_element) { return edge_element.object_acc; });

      std::vector<EdgeAccessor> out_edge_ordered_accessors;
      std::transform(out_ordered_edges.begin(), out_ordered_edges.end(), std::back_inserter(out_edge_ordered_accessors),
                     [](const auto &edge_element) { return edge_element.object_acc; });
      auto schema = shard_->GetSchema(shard_->PrimaryLabel());
      MG_ASSERT(schema);
      maybe_result =
          GetExpandOneResult(src_vertex_acc, src_vertice, req, in_edge_ordered_accessors, out_edge_ordered_accessors,
                             maybe_filter_based_on_edge_uniquness, edge_filler, *schema);
    }

    if (!maybe_result) {
      action_successful = false;
      break;
    }

    results.emplace_back(maybe_result.value());
    if (batch_limit.has_value() && results.size() >= batch_limit.value()) {
      break;
    }
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

}  // namespace memgraph::storage::v3
