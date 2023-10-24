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

#include "storage/v2/edge_accessor.hpp"

#include <memory>
#include <stdexcept>
#include <tuple>

#include "storage/v2/delta.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage {

bool EdgeAccessor::IsVisible(const View view) const {
  bool exists = true;
  bool deleted = true;
  // When edges don't have properties, their isolation level is still dictated by MVCC ->
  // iterate over the deltas of the from_vertex_ and see which deltas can be applied on edges.
  if (!storage_->config_.items.properties_on_edges) {
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
  if (!storage_->config_.items.properties_on_edges) return Error::PROPERTIES_DISABLED;

  auto guard = std::unique_lock{edge_.ptr->lock};

  if (!PrepareForWrite(transaction_, edge_.ptr)) return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  auto current_value = edge_.ptr->properties.GetProperty(property);
  // We could skip setting the value if the previous one is the same to the new
  // one. This would save some memory as a delta would not be created as well as
  // avoid copying the value. The reason we are not doing that is because the
  // current code always follows the logical pattern of "create a delta" and
  // "modify in-place". Additionally, the created delta will make other
  // transactions get a SERIALIZATION_ERROR.

  CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(), property, current_value);
  edge_.ptr->properties.SetProperty(property, value);

  if (transaction_->IsDiskStorage()) {
    ModifiedEdgeInfo modified_edge(Delta::Action::SET_PROPERTY, from_vertex_->gid, to_vertex_->gid, edge_type_, edge_);
    transaction_->AddModifiedEdge(Gid(), modified_edge);
  }

  return std::move(current_value);
}

Result<bool> EdgeAccessor::InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  if (!storage_->config_.items.properties_on_edges) return Error::PROPERTIES_DISABLED;

  auto guard = std::unique_lock{edge_.ptr->lock};

  if (!PrepareForWrite(transaction_, edge_.ptr)) return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  if (!edge_.ptr->properties.InitProperties(properties)) return false;
  for (const auto &[property, _] : properties) {
    CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(), property, PropertyValue());
  }

  return true;
}

Result<std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>>> EdgeAccessor::UpdateProperties(
    std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  if (!storage_->config_.items.properties_on_edges) return Error::PROPERTIES_DISABLED;

  auto guard = std::unique_lock{edge_.ptr->lock};

  if (!PrepareForWrite(transaction_, edge_.ptr)) return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  auto id_old_new_change = edge_.ptr->properties.UpdateProperties(properties);

  for (auto &[property, old_value, new_value] : id_old_new_change) {
    CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(), property, std::move(old_value));
  }

  return id_old_new_change;
}

Result<std::map<PropertyId, PropertyValue>> EdgeAccessor::ClearProperties() {
  if (!storage_->config_.items.properties_on_edges) return Error::PROPERTIES_DISABLED;

  auto guard = std::unique_lock{edge_.ptr->lock};

  if (!PrepareForWrite(transaction_, edge_.ptr)) return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  auto properties = edge_.ptr->properties.Properties();
  for (const auto &property : properties) {
    CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(), property.first, property.second);
  }

  edge_.ptr->properties.ClearProperties();

  return std::move(properties);
}

Result<PropertyValue> EdgeAccessor::GetProperty(PropertyId property, View view) const {
  if (!storage_->config_.items.properties_on_edges) return PropertyValue();
  bool exists = true;
  bool deleted = false;
  PropertyValue value;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{edge_.ptr->lock};
    deleted = edge_.ptr->deleted;
    value = edge_.ptr->properties.GetProperty(property);
    delta = edge_.ptr->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &value, property](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::SET_PROPERTY: {
        if (delta.property.key == property) {
          value = delta.property.value;
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
  return std::move(value);
}

Result<std::map<PropertyId, PropertyValue>> EdgeAccessor::Properties(View view) const {
  if (!storage_->config_.items.properties_on_edges) return std::map<PropertyId, PropertyValue>{};
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
          if (delta.property.value.IsNull()) {
            // remove the property
            properties.erase(it);
          } else {
            // set the value
            it->second = delta.property.value;
          }
        } else if (!delta.property.value.IsNull()) {
          properties.emplace(delta.property.key, delta.property.value);
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
  if (storage_->config_.items.properties_on_edges) {
    return edge_.ptr->gid;
  }
  return edge_.gid;
}

}  // namespace memgraph::storage
