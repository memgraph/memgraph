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

#include "storage/v2/vertex_accessor.hpp"

#include <memory>
#include <tuple>
#include <utility>

#include "query/exceptions.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/vertex_info_cache.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {

namespace detail {
std::pair<bool, bool> IsVisible(Vertex const *vertex, Transaction const *transaction, View view) {
  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex->lock};
    deleted = vertex->deleted;
    delta = vertex->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;

    if (useCache) {
      auto const &cache = transaction->manyDeltasCache;
      auto existsRes = cache.GetExists(view, vertex);
      auto deletedRes = cache.GetDeleted(view, vertex);
      if (existsRes && deletedRes) return {*existsRes, *deletedRes};
    }

    auto const n_processed = ApplyDeltasForRead(transaction, delta, view, [&](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
          Deleted_ActionMethod(deleted),
          Exists_ActionMethod(exists)
      });
      // clang-format on
    });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction->manyDeltasCache;
      cache.StoreExists(view, vertex, exists);
      cache.StoreDeleted(view, vertex, deleted);
    }
  }

  return {exists, deleted};
}
}  // namespace detail

std::optional<VertexAccessor> VertexAccessor::Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                                     Constraints *constraints, Config::Items config, View view) {
  if (const auto [exists, deleted] = detail::IsVisible(vertex, transaction, view); !exists || deleted) {
    return std::nullopt;
  }

  return VertexAccessor{vertex, transaction, indices, constraints, config};
}

bool VertexAccessor::IsVisible(const Vertex *vertex, const Transaction *transaction, View view) {
  const auto [exists, deleted] = detail::IsVisible(vertex, transaction, view);
  return exists && !deleted;
}

bool VertexAccessor::IsVisible(View view) const {
  const auto [exists, deleted] = detail::IsVisible(vertex_, transaction_, view);
  return exists && (for_deleted_ || !deleted);
}

Result<bool> VertexAccessor::AddLabel(LabelId label) {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;
  if (vertex_->deleted) return Error::DELETED_OBJECT;
  if (std::find(vertex_->labels.begin(), vertex_->labels.end(), label) != vertex_->labels.end()) return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::RemoveLabelTag(), label);
  vertex_->labels.push_back(label);

  /// TODO: some by pointers, some by reference => not good, make it better
  constraints_->unique_constraints_->UpdateOnAddLabel(label, *vertex_, transaction_->start_timestamp);
  transaction_->needs_constraint_verification = true;
  indices_->UpdateOnAddLabel(label, vertex_, *transaction_);
  transaction_->manyDeltasCache.Invalidate(vertex_, label);

  return true;
}

/// TODO: move to after update and change naming to vertex after update
Result<bool> VertexAccessor::RemoveLabel(LabelId label) {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;
  if (vertex_->deleted) return Error::DELETED_OBJECT;

  auto it = std::find(vertex_->labels.begin(), vertex_->labels.end(), label);
  if (it == vertex_->labels.end()) return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::AddLabelTag(), label);
  *it = vertex_->labels.back();
  vertex_->labels.pop_back();

  /// TODO: some by pointers, some by reference => not good, make it better
  constraints_->unique_constraints_->UpdateOnRemoveLabel(label, *vertex_, transaction_->start_timestamp);
  transaction_->needs_constraint_verification = true;
  indices_->UpdateOnRemoveLabel(label, vertex_, *transaction_);
  transaction_->manyDeltasCache.Invalidate(vertex_, label);

  return true;
}

Result<bool> VertexAccessor::HasLabel(LabelId label, View view) const {
  bool exists = true;
  bool deleted = false;
  bool has_label = false;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    has_label = std::find(vertex_->labels.begin(), vertex_->labels.end(), label) != vertex_->labels.end();
    delta = vertex_->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction_->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction_->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return *resError;
      if (auto resLabel = cache.GetHasLabel(view, vertex_, label); resLabel) return {resLabel.value()};
    }

    auto const n_processed = ApplyDeltasForRead(transaction_, delta, view, [&, label](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Deleted_ActionMethod(deleted),
        Exists_ActionMethod(exists),
        HasLabel_ActionMethod(has_label, label)
      });
      // clang-format on
    });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreHasLabel(view, vertex_, label, has_label);
    }
  }

  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return has_label;
}

Result<std::vector<LabelId>> VertexAccessor::Labels(View view) const {
  bool exists = true;
  bool deleted = false;
  std::vector<LabelId> labels;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    labels = vertex_->labels;
    delta = vertex_->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction_->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction_->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return *resError;
      if (auto resLabels = cache.GetLabels(view, vertex_); resLabels) return {*resLabels};
    }

    auto const n_processed = ApplyDeltasForRead(transaction_, delta, view, [&](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Deleted_ActionMethod(deleted),
        Exists_ActionMethod(exists),
        Labels_ActionMethod(labels)
      });
      // clang-format on
    });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreLabels(view, vertex_, labels);
    }
  }

  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return std::move(labels);
}

Result<PropertyValue> VertexAccessor::SetProperty(PropertyId property, const PropertyValue &value) {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }

  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  auto current_value = vertex_->properties.GetProperty(property);
  // We could skip setting the value if the previous one is the same to the new
  // one. This would save some memory as a delta would not be created as well as
  // avoid copying the value. The reason we are not doing that is because the
  // current code always follows the logical pattern of "create a delta" and
  // "modify in-place". Additionally, the created delta will make other
  // transactions get a SERIALIZATION_ERROR.

  CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property, current_value);
  vertex_->properties.SetProperty(property, value);

  transaction_->needs_constraint_verification = true;
  indices_->UpdateOnSetProperty(property, value, vertex_, *transaction_);
  transaction_->manyDeltasCache.Invalidate(vertex_, property);

  return std::move(current_value);
}

Result<bool> VertexAccessor::InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }

  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  if (!vertex_->properties.InitProperties(properties)) return false;
  for (const auto &[property, value] : properties) {
    CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property, PropertyValue());
    indices_->UpdateOnSetProperty(property, value, vertex_, *transaction_);
    transaction_->manyDeltasCache.Invalidate(vertex_, property);
  }
  transaction_->needs_constraint_verification = true;

  return true;
}

Result<std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>>> VertexAccessor::UpdateProperties(
    std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }

  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  auto id_old_new_change = vertex_->properties.UpdateProperties(properties);

  for (auto &[id, old_value, new_value] : id_old_new_change) {
    indices_->UpdateOnSetProperty(id, new_value, vertex_, *transaction_);
    CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), id, std::move(old_value));
    transaction_->manyDeltasCache.Invalidate(vertex_, id);
  }
  transaction_->needs_constraint_verification = true;

  return id_old_new_change;
}

Result<std::map<PropertyId, PropertyValue>> VertexAccessor::ClearProperties() {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  auto properties = vertex_->properties.Properties();
  for (const auto &[property, value] : properties) {
    CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property, value);
    indices_->UpdateOnSetProperty(property, PropertyValue(), vertex_, *transaction_);
    transaction_->manyDeltasCache.Invalidate(vertex_, property);
  }

  vertex_->properties.ClearProperties();

  return std::move(properties);
}

Result<PropertyValue> VertexAccessor::GetProperty(PropertyId property, View view) const {
  bool exists = true;
  bool deleted = false;
  PropertyValue value;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    value = vertex_->properties.GetProperty(property);
    delta = vertex_->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction_->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction_->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return *resError;
      if (auto resProperty = cache.GetProperty(view, vertex_, property); resProperty) return {*resProperty};
    }

    auto const n_processed =
        ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &value, property](const Delta &delta) {
          // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            PropertyValue_ActionMethod(value, property)
          });
          // clang-format on
        });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreProperty(view, vertex_, property, value);
    }
  }

  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return std::move(value);
}

Result<std::map<PropertyId, PropertyValue>> VertexAccessor::Properties(View view) const {
  bool exists = true;
  bool deleted = false;
  std::map<PropertyId, PropertyValue> properties;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    properties = vertex_->properties.Properties();
    delta = vertex_->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction_->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction_->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return *resError;
      if (auto resProperties = cache.GetProperties(view, vertex_); resProperties) return {*resProperties};
    }

    auto const n_processed =
        ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &properties](const Delta &delta) {
          // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            Properties_ActionMethod(properties)
          });
          // clang-format on
        });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreProperties(view, vertex_, properties);
    }
  }

  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return std::move(properties);
}

Result<EdgesVertexAccessorResult> VertexAccessor::InEdges(View view, const std::vector<EdgeTypeId> &edge_types,
                                                          const VertexAccessor *destination) const {
  MG_ASSERT(!destination || destination->transaction_ == transaction_, "Invalid accessor!");

  using edge_store = std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>>;

  // We return EdgeAccessors, this method with wrap the results in EdgeAccessors
  auto const build_result = [this](edge_store const &edges) -> std::vector<EdgeAccessor> {
    auto ret = std::vector<EdgeAccessor>{};
    ret.reserve(edges.size());
    for (auto const &[edge_type, from_vertex, edge] : edges) {
      ret.emplace_back(edge, edge_type, from_vertex, vertex_, transaction_, indices_, constraints_, config_);
    }
    return ret;
  };

  auto const *destination_vertex = destination ? destination->vertex_ : nullptr;

  bool exists = true;
  bool deleted = false;
  auto in_edges = edge_store{};
  Delta *delta = nullptr;
  int64_t expanded_count = 0;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    expanded_count = static_cast<int64_t>(vertex_->in_edges.size());
    // TODO: a better filter copy
    if (edge_types.empty() && !destination) {
      in_edges = vertex_->in_edges;
    } else {
      for (const auto &[edge_type, from_vertex, edge] : vertex_->in_edges) {
        if (destination && from_vertex != destination_vertex) continue;
        if (!edge_types.empty() && std::find(edge_types.begin(), edge_types.end(), edge_type) == edge_types.end())
          continue;
        in_edges.emplace_back(edge_type, from_vertex, edge);
      }
    }
    delta = vertex_->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction_->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction_->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return *resError;
      if (auto resInEdges = cache.GetInEdges(view, vertex_, destination_vertex, edge_types); resInEdges)
        return EdgesVertexAccessorResult{.edges = build_result(*resInEdges), .expanded_count = expanded_count};
    }

    auto const n_processed = ApplyDeltasForRead(
        transaction_, delta, view,
        [&exists, &deleted, &in_edges, &edge_types, &destination_vertex](const Delta &delta) {
          // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            Edges_ActionMethod<EdgeDirection::IN>(in_edges, edge_types, destination_vertex)
          });
          // clang-format on
        });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreInEdges(view, vertex_, destination_vertex, edge_types, in_edges);
    }
  }

  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (deleted) return Error::DELETED_OBJECT;

  return EdgesVertexAccessorResult{.edges = build_result(in_edges), .expanded_count = expanded_count};
}

Result<EdgesVertexAccessorResult> VertexAccessor::OutEdges(View view, const std::vector<EdgeTypeId> &edge_types,
                                                           const VertexAccessor *destination) const {
  MG_ASSERT(!destination || destination->transaction_ == transaction_, "Invalid accessor!");

  using edge_store = std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>>;

  auto const build_result = [this](edge_store const &out_edges) {
    auto ret = std::vector<EdgeAccessor>{};
    ret.reserve(out_edges.size());
    for (const auto &[edge_type, to_vertex, edge] : out_edges) {
      ret.emplace_back(edge, edge_type, vertex_, to_vertex, transaction_, indices_, constraints_, config_);
    }
    return ret;
  };

  auto const *dst_vertex = destination ? destination->vertex_ : nullptr;

  bool exists = true;
  bool deleted = false;
  auto out_edges = edge_store{};
  Delta *delta = nullptr;
  int64_t expanded_count = 0;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    expanded_count = static_cast<int64_t>(vertex_->out_edges.size());
    if (edge_types.empty() && !destination) {
      out_edges = vertex_->out_edges;
    } else {
      for (const auto &[edge_type, to_vertex, edge] : vertex_->out_edges) {
        if (destination && to_vertex != dst_vertex) continue;
        if (!edge_types.empty() && std::find(edge_types.begin(), edge_types.end(), edge_type) == edge_types.end())
          continue;
        out_edges.emplace_back(edge_type, to_vertex, edge);
      }
    }
    delta = vertex_->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction_->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction_->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return *resError;
      if (auto resOutEdges = cache.GetOutEdges(view, vertex_, dst_vertex, edge_types); resOutEdges)
        return EdgesVertexAccessorResult{.edges = build_result(*resOutEdges), .expanded_count = expanded_count};
    }

    auto const n_processed = ApplyDeltasForRead(
        transaction_, delta, view, [&exists, &deleted, &out_edges, &edge_types, &dst_vertex](const Delta &delta) {
          // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            Edges_ActionMethod<EdgeDirection::OUT>(out_edges, edge_types, dst_vertex)
          });
          // clang-format on
        });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreOutEdges(view, vertex_, dst_vertex, edge_types, out_edges);
    }
  }

  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (deleted) return Error::DELETED_OBJECT;

  return EdgesVertexAccessorResult{.edges = build_result(out_edges), .expanded_count = expanded_count};
}

Result<size_t> VertexAccessor::InDegree(View view) const {
  bool exists = true;
  bool deleted = false;
  size_t degree = 0;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    degree = vertex_->in_edges.size();
    delta = vertex_->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction_->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction_->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return *resError;
      if (auto resInDegree = cache.GetInDegree(view, vertex_); resInDegree) return {*resInDegree};
    }

    auto const n_processed =
        ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &degree](const Delta &delta) {
          // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            Degree_ActionMethod<EdgeDirection::IN>(degree)
          });
          // clang-format on
        });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreInDegree(view, vertex_, degree);
    }
  }

  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return degree;
}

Result<size_t> VertexAccessor::OutDegree(View view) const {
  bool exists = true;
  bool deleted = false;
  size_t degree = 0;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    degree = vertex_->out_edges.size();
    delta = vertex_->delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction_->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction_->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return *resError;
      if (auto resOutDegree = cache.GetOutDegree(view, vertex_); resOutDegree) return {*resOutDegree};
    }

    auto const n_processed =
        ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &degree](const Delta &delta) {
          // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            Degree_ActionMethod<EdgeDirection::OUT>(degree)
          });
          // clang-format on
        });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreOutDegree(view, vertex_, degree);
    }
  }

  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return degree;
}

}  // namespace memgraph::storage
