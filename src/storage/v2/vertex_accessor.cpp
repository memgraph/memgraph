// Copyright 2024 Memgraph Ltd.
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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <tuple>
#include <utility>

#include "query/exceptions.hpp"
#include "query/hops_limit.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/schema_info.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_info_cache.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "storage/v2/view.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/small_vector.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {

namespace {
void HandleTypeConstraintViolation(Storage const *storage, ConstraintViolation const &violation) {
  throw query::QueryException("IS TYPED {} violation on {}({})", TypeConstraintKindToString(*violation.constraint_kind),
                              storage->LabelToName(violation.label),
                              storage->PropertyToName(*violation.properties.begin()));
}
}  // namespace

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

std::optional<VertexAccessor> VertexAccessor::Create(Vertex *vertex, Storage *storage, Transaction *transaction,
                                                     View view) {
  if (const auto [exists, deleted] = detail::IsVisible(vertex, transaction, view); !exists || deleted) {
    return std::nullopt;
  }

  return VertexAccessor{vertex, storage, transaction};
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
  // This has to be called before any object gets locked
  // 1. do a optimistic shared lock
  // 2. lock vertex and check
  // 3. if unique lock is needed unlock both the vertex and accessor
  // 4. take unique access and re-lock vertex
  std::optional<std::variant<SchemaInfo::AnalyticalAccessor, SchemaInfo::AnalyticalUniqueAccessor>> schema_acc;
  if (auto schema_shared_acc = storage_->SchemaInfoAccessor(); schema_shared_acc) {
    schema_acc = std::move(*schema_shared_acc);
  }
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;
  if (vertex_->deleted) return Error::DELETED_OBJECT;

  // Now that the vertex is locked, we can check if it has any edges and if it does, we can upgrade the accessor
  if (schema_acc) {
    if (!vertex_->in_edges.empty() || !vertex_->out_edges.empty()) {
      guard.unlock();
      schema_acc.reset();
      schema_acc = *storage_->SchemaInfoUniqueAccessor();
      guard.lock();
      // Need to re-check for serialization errors
      if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;
      if (vertex_->deleted) return Error::DELETED_OBJECT;
    }
  }

  if (std::find(vertex_->labels.begin(), vertex_->labels.end(), label) != vertex_->labels.end()) return false;

  utils::AtomicMemoryBlock([transaction = transaction_, vertex = vertex_, &label]() {
    CreateAndLinkDelta(transaction, vertex, Delta::RemoveLabelTag(), label);
    vertex->labels.push_back(label);
  });

  if (storage_->constraints_.HasTypeConstraints()) {
    if (auto maybe_violation = storage_->constraints_.type_constraints_->Validate(*vertex_, label)) {
      HandleTypeConstraintViolation(storage_, *maybe_violation);
    }
  }

  if (storage_->config_.salient.items.enable_schema_metadata) {
    storage_->stored_node_labels_.try_insert(label);
  }

  if (storage_->config_.salient.items.enable_label_index_auto_creation &&
      !storage_->indices_.label_index_->IndexExists(label)) {
    storage_->labels_to_auto_index_.WithLock([&](auto &label_indices) {
      if (auto it = label_indices.find(label); it != label_indices.end()) {
        const bool this_txn_already_encountered_label = transaction_->introduced_new_label_index_.contains(label);
        if (!this_txn_already_encountered_label) {
          ++(it->second);
        }
        return;
      }
      label_indices.insert({label, 1});
    });
    transaction_->introduced_new_label_index_.insert(label);
  }

  /// TODO: some by pointers, some by reference => not good, make it better
  storage_->constraints_.unique_constraints_->UpdateOnAddLabel(label, *vertex_, transaction_->start_timestamp);
  if (transaction_->constraint_verification_info) transaction_->constraint_verification_info->AddedLabel(vertex_);
  storage_->indices_.UpdateOnAddLabel(label, vertex_, *transaction_);
  transaction_->UpdateOnChangeLabel(label, vertex_);

  // NOTE Has to be called at the end because it needs to be able to release the vertex lock (in case edges need to be
  // updated)
  if (schema_acc) {
    std::visit(utils::Overloaded(
                   [&](SchemaInfo::AnalyticalAccessor &acc) { acc.AddLabel(vertex_, label); },
                   [&](SchemaInfo::AnalyticalUniqueAccessor &acc) { acc.AddLabel(vertex_, label, std::move(guard)); }),
               *schema_acc);
  }
  return true;
}

/// TODO: move to after update and change naming to vertex after update
Result<bool> VertexAccessor::RemoveLabel(LabelId label) {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }
  // This has to be called before any object gets locked
  // 1. do an optimistic shared lock
  // 2. lock vertex and check
  // 3. if unique lock is needed unlock both the vertex and accessor
  // 4. take unique access and re-lock vertex
  std::optional<std::variant<SchemaInfo::AnalyticalAccessor, SchemaInfo::AnalyticalUniqueAccessor>> schema_acc;
  if (auto schema_shared_acc = storage_->SchemaInfoAccessor(); schema_shared_acc) {
    schema_acc = std::move(*schema_shared_acc);
  }
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;
  if (vertex_->deleted) return Error::DELETED_OBJECT;

  // Now that the vertex is locked, we can check if it has any edges and if it does, we can upgrade the accessor
  if (schema_acc) {
    if (!vertex_->in_edges.empty() || !vertex_->out_edges.empty()) {
      guard.unlock();
      schema_acc.reset();
      schema_acc = *storage_->SchemaInfoUniqueAccessor();
      guard.lock();
      // Need to re-check for serialization errors
      if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;
      if (vertex_->deleted) return Error::DELETED_OBJECT;
    }
  }

  auto it = std::find(vertex_->labels.begin(), vertex_->labels.end(), label);
  if (it == vertex_->labels.end()) return false;

  utils::AtomicMemoryBlock([transaction = transaction_, vertex = vertex_, &label, &it]() {
    CreateAndLinkDelta(transaction, vertex, Delta::AddLabelTag(), label);
    *it = vertex->labels.back();
    vertex->labels.pop_back();
  });

  /// TODO: some by pointers, some by reference => not good, make it better
  storage_->constraints_.unique_constraints_->UpdateOnRemoveLabel(label, *vertex_, transaction_->start_timestamp);
  storage_->indices_.UpdateOnRemoveLabel(label, vertex_, *transaction_);
  transaction_->UpdateOnChangeLabel(label, vertex_);

  // NOTE Has to be called at the end because it needs to be able to release the vertex lock (in case edges need to be
  // updated)
  if (schema_acc) {
    std::visit(utils::Overloaded([&](SchemaInfo::AnalyticalAccessor &acc) { acc.RemoveLabel(vertex_, label); },
                                 [&](SchemaInfo::AnalyticalUniqueAccessor &acc) {
                                   acc.RemoveLabel(vertex_, label, std::move(guard));
                                 }),
               *schema_acc);
  }
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

Result<utils::small_vector<LabelId>> VertexAccessor::Labels(View view) const {
  bool exists = true;
  bool deleted = false;
  utils::small_vector<LabelId> labels;
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

Result<PropertyValue> VertexAccessor::SetProperty(PropertyId property, const PropertyValue &new_value) const {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }

  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  // This has to be called before any object gets locked
  auto schema_acc = storage_->SchemaInfoAccessor();
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  PropertyValue old_value;
  const bool skip_duplicate_write = !storage_->config_.salient.items.delta_on_identical_property_update;
  auto const set_property_impl = [this, transaction = transaction_, vertex = vertex_, &new_value, &property, &old_value,
                                  skip_duplicate_write, &schema_acc]() {
    old_value = vertex->properties.GetProperty(property);
    // We could skip setting the value if the previous one is the same to the new
    // one. This would save some memory as a delta would not be created as well as
    // avoid copying the value. The reason we are not doing that is because the
    // current code always follows the logical pattern of "create a delta" and
    // "modify in-place". Additionally, the created delta will make other
    // transactions get a SERIALIZATION_ERROR.
    if (skip_duplicate_write && old_value == new_value) {
      return true;
    }

    CreateAndLinkDelta(transaction, vertex, Delta::SetPropertyTag(), property, old_value);
    vertex->properties.SetProperty(property, new_value);
    if (schema_acc)
      schema_acc->SetProperty(vertex, property, ExtendedPropertyType{new_value}, ExtendedPropertyType{old_value});

    if (storage_->constraints_.HasTypeConstraints()) {
      if (auto maybe_violation = storage_->constraints_.type_constraints_->Validate(*vertex_, property, new_value)) {
        HandleTypeConstraintViolation(storage_, *maybe_violation);
      }
    }

    return false;
  };

  auto early_exit = utils::AtomicMemoryBlock(set_property_impl);
  if (early_exit) {
    return std::move(old_value);
  }

  if (transaction_->constraint_verification_info) {
    if (!new_value.IsNull()) {
      transaction_->constraint_verification_info->AddedProperty(vertex_);
    } else {
      transaction_->constraint_verification_info->RemovedProperty(vertex_);
    }
  }
  storage_->indices_.UpdateOnSetProperty(property, new_value, vertex_, *transaction_);
  transaction_->UpdateOnSetProperty(property, old_value, new_value, vertex_);

  return std::move(old_value);
}

Result<bool> VertexAccessor::InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }

  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  // This has to be called before any object gets locked
  auto schema_acc = storage_->SchemaInfoAccessor();
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;
  bool result{false};
  utils::AtomicMemoryBlock(
      [&result, &properties, storage = storage_, transaction = transaction_, vertex = vertex_, &schema_acc]() {
        if (!vertex->properties.InitProperties(properties)) {
          result = false;
          return;
        }
        for (const auto &[property, new_value] : properties) {
          CreateAndLinkDelta(transaction, vertex, Delta::SetPropertyTag(), property, PropertyValue());
          storage->indices_.UpdateOnSetProperty(property, new_value, vertex, *transaction);
          transaction->UpdateOnSetProperty(property, PropertyValue{}, new_value, vertex);
          if (transaction->constraint_verification_info) {
            if (!new_value.IsNull()) {
              transaction->constraint_verification_info->AddedProperty(vertex);
            } else {
              transaction->constraint_verification_info->RemovedProperty(vertex);
            }
          }
          if (schema_acc)
            schema_acc->SetProperty(vertex, property, ExtendedPropertyType{new_value}, ExtendedPropertyType{});
        }
        // TODO If not performant enough there is also InitProperty()
        if (storage->constraints_.HasTypeConstraints()) {
          for (auto const &[property_id, property_value] : properties) {
            if (auto maybe_violation =
                    storage->constraints_.type_constraints_->Validate(*vertex, property_id, property_value)) {
              HandleTypeConstraintViolation(storage, *maybe_violation);
            }
          }
        }
        result = true;
      });

  return result;
}

Result<std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>>> VertexAccessor::UpdateProperties(
    std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }

  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  // This has to be called before any object gets locked
  auto schema_acc = storage_->SchemaInfoAccessor();
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  const bool skip_duplicate_update = storage_->config_.salient.items.delta_on_identical_property_update;
  using ReturnType = decltype(vertex_->properties.UpdateProperties(properties));
  std::optional<ReturnType> id_old_new_change;
  utils::AtomicMemoryBlock([storage = storage_, transaction = transaction_, vertex = vertex_, &properties,
                            &id_old_new_change, skip_duplicate_update, &schema_acc]() {
    id_old_new_change.emplace(vertex->properties.UpdateProperties(properties));
    if (!id_old_new_change.has_value()) {
      return;
    }

    for (auto &[id, old_value, new_value] : *id_old_new_change) {
      storage->indices_.UpdateOnSetProperty(id, new_value, vertex, *transaction);
      if (skip_duplicate_update && old_value == new_value) continue;
      CreateAndLinkDelta(transaction, vertex, Delta::SetPropertyTag(), id, old_value);
      transaction->UpdateOnSetProperty(id, old_value, new_value, vertex);
      if (transaction->constraint_verification_info) {
        if (!new_value.IsNull()) {
          transaction->constraint_verification_info->AddedProperty(vertex);
        } else {
          transaction->constraint_verification_info->RemovedProperty(vertex);
        }
      }
      if (schema_acc)
        schema_acc->SetProperty(vertex, id, ExtendedPropertyType{new_value}, ExtendedPropertyType{old_value});
    }
    if (storage->constraints_.HasTypeConstraints()) {
      if (auto maybe_violation = storage->constraints_.type_constraints_->Validate(*vertex)) {
        HandleTypeConstraintViolation(storage, *maybe_violation);
      }
    }
  });

  return id_old_new_change.has_value() ? std::move(id_old_new_change.value()) : ReturnType{};
}

Result<std::map<PropertyId, PropertyValue>> VertexAccessor::ClearProperties() {
  if (transaction_->edge_import_mode_active) {
    throw query::WriteVertexOperationInEdgeImportModeException();
  }
  // This has to be called before any object gets locked
  auto schema_acc = storage_->SchemaInfoAccessor();
  auto guard = std::unique_lock{vertex_->lock};

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  using ReturnType = decltype(vertex_->properties.Properties());
  std::optional<ReturnType> properties;
  utils::AtomicMemoryBlock(
      [storage = storage_, transaction = transaction_, vertex = vertex_, &properties, &schema_acc]() {
        properties.emplace(vertex->properties.Properties());
        if (!properties.has_value()) {
          return;
        }
        auto new_value = PropertyValue();
        for (const auto &[property, old_value] : *properties) {
          CreateAndLinkDelta(transaction, vertex, Delta::SetPropertyTag(), property, old_value);
          storage->indices_.UpdateOnSetProperty(property, new_value, vertex, *transaction);
          transaction->UpdateOnSetProperty(property, old_value, new_value, vertex);
          if (schema_acc)
            schema_acc->SetProperty(vertex, property, ExtendedPropertyType{}, ExtendedPropertyType{old_value});
        }
        if (transaction->constraint_verification_info) {
          transaction->constraint_verification_info->RemovedProperty(vertex);
        }
        vertex->properties.ClearProperties();
      });

  return properties.has_value() ? std::move(properties.value()) : ReturnType{};
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

Result<uint64_t> VertexAccessor::GetPropertySize(PropertyId property, View view) const {
  {
    auto guard = std::shared_lock{vertex_->lock};
    Delta *delta = vertex_->delta;
    if (!delta) {
      return vertex_->properties.PropertySize(property);
    }
  }

  auto property_result = this->GetProperty(property, view);
  if (property_result.HasError()) {
    return property_result.GetError();
  }

  auto property_store = storage::PropertyStore();
  property_store.SetProperty(property, *property_result);

  return property_store.PropertySize(property);
};

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

auto VertexAccessor::BuildResultOutEdges(edge_store const &out_edges) const {
  auto ret = std::vector<EdgeAccessor>{};
  ret.reserve(out_edges.size());
  for (const auto &[edge_type, to_vertex, edge] : out_edges) {
    ret.emplace_back(edge, edge_type, vertex_, to_vertex, storage_, transaction_);
  }
  return ret;
};

auto VertexAccessor::BuildResultInEdges(edge_store const &out_edges) const {
  auto ret = std::vector<EdgeAccessor>{};
  ret.reserve(out_edges.size());
  for (const auto &[edge_type, from_vertex, edge] : out_edges) {
    ret.emplace_back(edge, edge_type, from_vertex, vertex_, storage_, transaction_);
  }
  return ret;
};

auto VertexAccessor::BuildResultWithDisk(edge_store const &in_memory_edges, std::vector<EdgeAccessor> const &disk_edges,
                                         View view, const std::string &mode) const {
  /// TODO: (andi) Better mode handling
  auto ret = std::invoke([this, &mode, &in_memory_edges]() {
    if (mode == "OUT") {
      return BuildResultOutEdges(in_memory_edges);
    }
    return BuildResultInEdges(in_memory_edges);
  });
  /// TODO: (andi) Maybe this check can be done in build_result without damaging anything else.
  std::erase_if(ret, [transaction = this->transaction_, view](const EdgeAccessor &edge_acc) {
    return !edge_acc.IsVisible(view) || !edge_acc.FromVertex().IsVisible(view) ||
           !edge_acc.ToVertex().IsVisible(view) || transaction->edges_to_delete_.contains(edge_acc.Gid().ToString());
  });
  std::unordered_set<storage::Gid> in_mem_edges_set;
  in_mem_edges_set.reserve(ret.size());
  for (const auto &in_mem_edge_acc : ret) {
    in_mem_edges_set.insert(in_mem_edge_acc.Gid());
  }

  for (const auto &disk_edge_acc : disk_edges) {
    auto const edge_gid_str = disk_edge_acc.Gid().ToString();
    if (in_mem_edges_set.contains(disk_edge_acc.Gid()) ||
        (view == View::NEW && transaction_->edges_to_delete_.contains(edge_gid_str))) {
      continue;
    }
    ret.emplace_back(disk_edge_acc);
  }
  return ret;
};

Result<EdgesVertexAccessorResult> VertexAccessor::InEdges(View view, const std::vector<EdgeTypeId> &edge_types,
                                                          const VertexAccessor *destination,
                                                          query::HopsLimit *hops_limit) const {
  MG_ASSERT(!destination || destination->transaction_ == transaction_, "Invalid accessor!");

  std::vector<EdgeAccessor> disk_edges{};

  /// TODO: (andi) I think that here should be another check:
  /// in memory storage should be checked only if something exists before loading from the disk.
  if (transaction_->IsDiskStorage()) {
    auto *disk_storage = static_cast<DiskStorage *>(storage_);
    const auto [exists, deleted] = detail::IsVisible(vertex_, transaction_, view);
    if (!exists) return Error::NONEXISTENT_OBJECT;
    if (deleted) return Error::DELETED_OBJECT;
    bool edges_modified_in_tx = !vertex_->in_edges.empty();

    disk_edges = disk_storage->InEdges(this, edge_types, destination, transaction_, view, hops_limit);
    if (view == View::OLD && !edges_modified_in_tx) {
      return EdgesVertexAccessorResult{.edges = disk_edges, .expanded_count = static_cast<int64_t>(disk_edges.size())};
    }
  }

  auto const *destination_vertex = destination ? destination->vertex_ : nullptr;

  bool exists = true;
  bool deleted = false;
  auto in_edges = edge_store{};
  Delta *delta = nullptr;
  int64_t expanded_count = 0;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    if (edge_types.empty() && !destination) {
      expanded_count = HandleExpansionsWithoutEdgeTypes(in_edges, hops_limit, EdgeDirection::IN);
    } else {
      expanded_count = HandleExpansionsWithEdgeTypes(in_edges, edge_types, destination, hops_limit, EdgeDirection::IN);
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
        return EdgesVertexAccessorResult{.edges = BuildResultInEdges(*resInEdges), .expanded_count = expanded_count};
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

  if (transaction_->IsDiskStorage()) {
    return EdgesVertexAccessorResult{.edges = BuildResultWithDisk(in_edges, disk_edges, view, "IN"),
                                     .expanded_count = expanded_count};
  }

  return EdgesVertexAccessorResult{.edges = BuildResultInEdges(in_edges), .expanded_count = expanded_count};
}

Result<EdgesVertexAccessorResult> VertexAccessor::OutEdges(View view, const std::vector<EdgeTypeId> &edge_types,
                                                           const VertexAccessor *destination,
                                                           query::HopsLimit *hops_limit) const {
  MG_ASSERT(!destination || destination->transaction_ == transaction_, "Invalid accessor!");

  /// TODO: (andi) I think that here should be another check:
  /// in memory storage should be checked only if something exists before loading from the disk.
  std::vector<EdgeAccessor> disk_edges{};
  if (transaction_->IsDiskStorage()) {
    auto *disk_storage = static_cast<DiskStorage *>(storage_);
    const auto [exists, deleted] = detail::IsVisible(vertex_, transaction_, view);
    if (!exists) return Error::NONEXISTENT_OBJECT;
    if (deleted) return Error::DELETED_OBJECT;
    bool edges_modified_in_tx = !vertex_->out_edges.empty();

    disk_edges = disk_storage->OutEdges(this, edge_types, destination, transaction_, view, hops_limit);

    if (view == View::OLD && !edges_modified_in_tx) {
      return EdgesVertexAccessorResult{.edges = disk_edges, .expanded_count = static_cast<int64_t>(disk_edges.size())};
    }
  }

  auto const *dst_vertex = destination ? destination->vertex_ : nullptr;

  bool exists = true;
  bool deleted = false;
  auto out_edges = edge_store{};
  Delta *delta = nullptr;
  int64_t expanded_count = 0;
  {
    auto guard = std::shared_lock{vertex_->lock};
    deleted = vertex_->deleted;
    if (edge_types.empty() && !destination) {
      expanded_count = HandleExpansionsWithoutEdgeTypes(out_edges, hops_limit, EdgeDirection::OUT);
    } else {
      expanded_count =
          HandleExpansionsWithEdgeTypes(out_edges, edge_types, destination, hops_limit, EdgeDirection::OUT);
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
        return EdgesVertexAccessorResult{.edges = BuildResultOutEdges(*resOutEdges), .expanded_count = expanded_count};
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

  if (transaction_->IsDiskStorage()) {
    return EdgesVertexAccessorResult{.edges = BuildResultWithDisk(out_edges, disk_edges, view, "OUT"),
                                     .expanded_count = expanded_count};
  }
  /// InMemoryStorage
  return EdgesVertexAccessorResult{.edges = BuildResultOutEdges(out_edges), .expanded_count = expanded_count};
}

Result<size_t> VertexAccessor::InDegree(View view) const {
  std::vector<EdgeAccessor> disk_edges{};
  if (transaction_->IsDiskStorage()) {
    auto res = InEdges(view);
    if (res.HasValue()) {
      return res->edges.size();
    }
    return res.GetError();
  }

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
  if (transaction_->IsDiskStorage()) {
    auto res = OutEdges(view);
    if (res.HasValue()) {
      return res->edges.size();
    }
    return res.GetError();
  }

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

int64_t VertexAccessor::HandleExpansionsWithoutEdgeTypes(edge_store &result_edges, query::HopsLimit *hops_limit,
                                                         EdgeDirection direction) const {
  int64_t expanded_count = 0;
  const auto &edges = direction == EdgeDirection::IN ? vertex_->in_edges : vertex_->out_edges;
  if (hops_limit && hops_limit->IsUsed()) {
    if (hops_limit->LeftHops() == 0 && static_cast<int64_t>(edges.size()) > 0) {
      hops_limit->limit_reached = true;
    } else {
      expanded_count = std::min(hops_limit->LeftHops(), static_cast<int64_t>(edges.size()));
      hops_limit->IncrementHopsCount(expanded_count);
      std::copy_n(edges.begin(), expanded_count, std::back_inserter(result_edges));
    }
  } else {
    expanded_count = static_cast<int64_t>(edges.size());
    result_edges = edges;
  }
  return expanded_count;
}

int64_t VertexAccessor::HandleExpansionsWithEdgeTypes(edge_store &result_edges,
                                                      const std::vector<EdgeTypeId> &edge_types,
                                                      const VertexAccessor *destination, query::HopsLimit *hops_limit,
                                                      EdgeDirection direction) const {
  int64_t expanded_count = 0;
  const auto &edges = direction == EdgeDirection::IN ? vertex_->in_edges : vertex_->out_edges;
  for (const auto &[edge_type, vertex, edge] : edges) {
    if (hops_limit && hops_limit->IsUsed()) {
      hops_limit->IncrementHopsCount(1);
      if (hops_limit->IsLimitReached()) break;
    }
    expanded_count++;
    if (destination && vertex != destination->vertex_) continue;
    if (!edge_types.empty() && std::find(edge_types.begin(), edge_types.end(), edge_type) == edge_types.end()) continue;
    result_edges.emplace_back(edge_type, vertex, edge);
  }
  return expanded_count;
}
}  // namespace memgraph::storage
