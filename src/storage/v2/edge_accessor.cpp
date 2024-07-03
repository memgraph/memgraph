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

#include "storage/v2/edge_accessor.hpp"

#include <memory>
#include <stdexcept>
#include <tuple>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_info_helpers.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage {
std::optional<EdgeAccessor> EdgeAccessor::Create(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex,
                                                 Vertex *to_vertex, Storage *storage, Transaction *transaction,
                                                 View view, bool for_deleted) {
  if (!IsEdgeVisible(edge.ptr, transaction, view)) {
    return std::nullopt;
  }

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, storage, transaction, for_deleted);
}

bool EdgeAccessor::IsDeleted() const {
  if (!storage_->config_.salient.items.properties_on_edges) {
    return false;
  }
  return edge_.ptr->deleted;
}

bool EdgeAccessor::IsVisible(const View view) const {
  bool exists = true;
  bool deleted = true;
  // When edges don't have properties, their isolation level is still dictated by MVCC ->
  // iterate over the deltas of the from_vertex_ and see which deltas can be applied on edges.
  if (!storage_->config_.salient.items.properties_on_edges) {
    Delta *delta = nullptr;
    {
      auto guard = std::shared_lock{from_vertex_->lock};
      // Initialize deleted by checking if out edges contain edge_
      deleted = std::find_if(from_vertex_->out_edges.begin(), from_vertex_->out_edges.end(), [&](const auto &out_edge) {
                  return std::get<2>(out_edge) == edge_;
                }) == from_vertex_->out_edges.end();
      delta = from_vertex_->delta;
    }
    ApplyDeltasForRead(transaction_, delta, view, [&](const Delta &delta) {
      switch (delta.action) {
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
        case Delta::Action::DELETE_OBJECT:
          break;
        case Delta::Action::ADD_OUT_EDGE: {  // relevant for the from_vertex_ -> we just deleted the edge
          if (delta.vertex_edge.edge == edge_) {
            deleted = false;
          }
          break;
        }
        case Delta::Action::REMOVE_OUT_EDGE: {  // also relevant for the from_vertex_ -> we just added the edge
          if (delta.vertex_edge.edge == edge_) {
            exists = false;
          }
          break;
        }
      }
    });
    return exists && (for_deleted_ || !deleted);
  }

  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{edge_.ptr->lock};
    deleted = edge_.ptr->deleted;
    delta = edge_.ptr->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
    }
  });
  return exists && (for_deleted_ || !deleted);
}

VertexAccessor EdgeAccessor::FromVertex() const { return VertexAccessor{from_vertex_, storage_, transaction_}; }

VertexAccessor EdgeAccessor::ToVertex() const { return VertexAccessor{to_vertex_, storage_, transaction_}; }

VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const {
  return VertexAccessor{from_vertex_, storage_, transaction_, for_deleted_ && from_vertex_->deleted};
}

VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const {
  return VertexAccessor{to_vertex_, storage_, transaction_, for_deleted_ && to_vertex_->deleted};
}

Result<storage::PropertyValue> EdgeAccessor::SetProperty(PropertyId property, const PropertyValue &value) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  if (!storage_->config_.salient.items.properties_on_edges) return Error::PROPERTIES_DISABLED;

  auto guard = std::unique_lock{edge_.ptr->lock};

  if (!PrepareForWrite(transaction_, edge_.ptr)) return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;
  using ReturnType = decltype(edge_.ptr->properties.GetProperty(property));
  std::optional<ReturnType> current_value;
  const bool skip_duplicate_write = !storage_->config_.salient.items.delta_on_identical_property_update;
  utils::AtomicMemoryBlock(
      [this, &current_value, &property, &value, transaction = transaction_, edge = edge_, skip_duplicate_write]() {
        current_value.emplace(edge.ptr->properties.GetProperty(property));
        if (skip_duplicate_write && current_value == value) {
          return;
        }
        // We could skip setting the value if the previous one is the same to the new
        // one. This would save some memory as a delta would not be created as well as
        // avoid copying the value. The reason we are not doing that is because the
        // current code always follows the logical pattern of "create a delta" and
        // "modify in-place". Additionally, the created delta will make other
        // transactions get a SERIALIZATION_ERROR.
        CreateAndLinkDelta(transaction, edge.ptr, Delta::SetPropertyTag(), property, *current_value);
        edge.ptr->properties.SetProperty(property, value);
        storage_->indices_.UpdateOnSetProperty(edge_type_, property, value, from_vertex_, to_vertex_, edge_.ptr,
                                               *transaction_);
      });

  if (transaction_->IsDiskStorage()) {
    ModifiedEdgeInfo modified_edge(Delta::Action::SET_PROPERTY, from_vertex_->gid, to_vertex_->gid, edge_type_, edge_);
    transaction_->AddModifiedEdge(Gid(), modified_edge);
  }

  return std::move(*current_value);
}

Result<bool> EdgeAccessor::InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  if (!storage_->config_.salient.items.properties_on_edges) return Error::PROPERTIES_DISABLED;

  auto guard = std::unique_lock{edge_.ptr->lock};

  if (!PrepareForWrite(transaction_, edge_.ptr)) return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  if (!edge_.ptr->properties.InitProperties(properties)) return false;
  utils::AtomicMemoryBlock([this, &properties, transaction_ = transaction_, edge_ = edge_]() {
    for (const auto &[property, value] : properties) {
      CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(), property, PropertyValue());
      storage_->indices_.UpdateOnSetProperty(edge_type_, property, value, from_vertex_, to_vertex_, edge_.ptr,
                                             *transaction_);
    }
  });

  return true;
}

Result<std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>>> EdgeAccessor::UpdateProperties(
    std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  if (!storage_->config_.salient.items.properties_on_edges) return Error::PROPERTIES_DISABLED;

  auto guard = std::unique_lock{edge_.ptr->lock};

  if (!PrepareForWrite(transaction_, edge_.ptr)) return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  const bool skip_duplicate_write = !storage_->config_.salient.items.delta_on_identical_property_update;
  using ReturnType = decltype(edge_.ptr->properties.UpdateProperties(properties));
  std::optional<ReturnType> id_old_new_change;
  utils::AtomicMemoryBlock([this, &properties, &id_old_new_change, skip_duplicate_write]() {
    id_old_new_change.emplace(edge_.ptr->properties.UpdateProperties(properties));
    for (auto &[property, old_value, new_value] : *id_old_new_change) {
      if (skip_duplicate_write && old_value == new_value) continue;
      CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(), property, std::move(old_value));
      storage_->indices_.UpdateOnSetProperty(edge_type_, property, new_value, from_vertex_, to_vertex_, edge_.ptr,
                                             *transaction_);
    }
  });

  return id_old_new_change.has_value() ? std::move(id_old_new_change.value()) : ReturnType{};
}

Result<std::map<PropertyId, PropertyValue>> EdgeAccessor::ClearProperties() {
  if (!storage_->config_.salient.items.properties_on_edges) return Error::PROPERTIES_DISABLED;

  auto guard = std::unique_lock{edge_.ptr->lock};

  if (!PrepareForWrite(transaction_, edge_.ptr)) return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  using ReturnType = decltype(edge_.ptr->properties.Properties());
  std::optional<ReturnType> properties;
  utils::AtomicMemoryBlock([&properties, this]() {
    properties.emplace(edge_.ptr->properties.Properties());
    for (const auto &property : *properties) {
      CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(), property.first, property.second);
      storage_->indices_.UpdateOnSetProperty(edge_type_, property.first, PropertyValue(), from_vertex_, to_vertex_,
                                             edge_.ptr, *transaction_);
    }

    edge_.ptr->properties.ClearProperties();
  });

  return properties.has_value() ? std::move(properties.value()) : ReturnType{};
}

Result<PropertyValue> EdgeAccessor::GetProperty(PropertyId property, View view) const {
  if (!storage_->config_.salient.items.properties_on_edges) return PropertyValue();
  bool exists = true;
  bool deleted = false;
  std::optional<PropertyValue> value;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{edge_.ptr->lock};
    deleted = edge_.ptr->deleted;
    value.emplace(edge_.ptr->properties.GetProperty(property));
    delta = edge_.ptr->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &value, property](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::SET_PROPERTY: {
        if (delta.property.key == property) {
          value = *delta.property.value;
        }
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return *std::move(value);
}

Result<uint64_t> EdgeAccessor::GetPropertySize(PropertyId property, View view) const {
  if (!storage_->config_.salient.items.properties_on_edges) return 0;

  auto guard = std::shared_lock{edge_.ptr->lock};
  Delta *delta = edge_.ptr->delta;
  if (!delta) {
    return edge_.ptr->properties.PropertySize(property);
  }

  auto property_result = this->GetProperty(property, view);

  if (property_result.HasError()) {
    return property_result.GetError();
  }

  auto property_store = storage::PropertyStore();
  property_store.SetProperty(property, *property_result);

  return property_store.PropertySize(property);
};

Result<std::map<PropertyId, PropertyValue>> EdgeAccessor::Properties(View view) const {
  if (!storage_->config_.salient.items.properties_on_edges) return std::map<PropertyId, PropertyValue>{};
  bool exists = true;
  bool deleted = false;
  std::map<PropertyId, PropertyValue> properties;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{edge_.ptr->lock};
    deleted = edge_.ptr->deleted;
    properties = edge_.ptr->properties.Properties();
    delta = edge_.ptr->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &properties](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::SET_PROPERTY: {
        auto it = properties.find(delta.property.key);
        if (it != properties.end()) {
          if (delta.property.value->IsNull()) {
            // remove the property
            properties.erase(it);
          } else {
            // set the value
            it->second = *delta.property.value;
          }
        } else if (!delta.property.value->IsNull()) {
          properties.emplace(delta.property.key, *delta.property.value);
        }
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return std::move(properties);
}

Gid EdgeAccessor::Gid() const noexcept {
  if (storage_->config_.salient.items.properties_on_edges) {
    return edge_.ptr->gid;
  }
  return edge_.gid;
}

}  // namespace memgraph::storage
