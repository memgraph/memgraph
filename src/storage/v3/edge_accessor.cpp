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

#include "storage/v3/edge_accessor.hpp"

#include <memory>

#include "storage/v3/mvcc.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage::v3 {

bool EdgeAccessor::IsVisible(const View view) const {
  bool deleted = true;
  bool exists = true;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);
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
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
    }
  });

  return exists && (for_deleted_ || !deleted);
}

VertexAccessor EdgeAccessor::FromVertex() const {
  return {from_vertex_, transaction_, indices_, constraints_, config_, *schema_validator_, *schemas_};
}

VertexAccessor EdgeAccessor::ToVertex() const {
  return {to_vertex_, transaction_, indices_, constraints_, config_, *schema_validator_, *schemas_};
}

Result<PropertyValue> EdgeAccessor::SetProperty(PropertyId property, const PropertyValue &value) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  if (!config_.properties_on_edges) return Error::PROPERTIES_DISABLED;

  std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);

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

  return std::move(current_value);
}

Result<std::map<PropertyId, PropertyValue>> EdgeAccessor::ClearProperties() {
  if (!config_.properties_on_edges) return Error::PROPERTIES_DISABLED;

  std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);

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
  if (!config_.properties_on_edges) return PropertyValue();
  bool exists = true;
  bool deleted = false;
  PropertyValue value;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);
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
  if (!config_.properties_on_edges) return std::map<PropertyId, PropertyValue>{};
  bool exists = true;
  bool deleted = false;
  std::map<PropertyId, PropertyValue> properties;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);
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

}  // namespace memgraph::storage::v3
