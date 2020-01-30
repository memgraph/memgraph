#include "storage/v2/edge_accessor.hpp"

#include <memory>

#include "storage/v2/mvcc.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace storage {

VertexAccessor EdgeAccessor::FromVertex() const {
  return VertexAccessor{from_vertex_, transaction_, indices_, constraints_, config_};
}

VertexAccessor EdgeAccessor::ToVertex() const {
  return VertexAccessor{to_vertex_, transaction_, indices_, constraints_, config_};
}

Result<bool> EdgeAccessor::SetProperty(PropertyId property,
                                       const PropertyValue &value) {
  if (!config_.properties_on_edges) return Error::PROPERTIES_DISABLED;

  std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);

  if (!PrepareForWrite(transaction_, edge_.ptr))
    return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  auto it = edge_.ptr->properties.find(property);
  bool existed = it != edge_.ptr->properties.end();
  // We could skip setting the value if the previous one is the same to the new
  // one. This would save some memory as a delta would not be created as well as
  // avoid copying the value. The reason we are not doing that is because the
  // current code always follows the logical pattern of "create a delta" and
  // "modify in-place". Additionally, the created delta will make other
  // transactions get a SERIALIZATION_ERROR.
  if (it != edge_.ptr->properties.end()) {
    CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(),
                       property, it->second);
    if (value.IsNull()) {
      // remove the property
      edge_.ptr->properties.erase(it);
    } else {
      // set the value
      it->second = value;
    }
  } else {
    CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(),
                       property, PropertyValue());
    if (!value.IsNull()) {
      edge_.ptr->properties.emplace(property, value);
    }
  }

  return !existed;
}

Result<bool> EdgeAccessor::ClearProperties() {
  if (!config_.properties_on_edges) return Error::PROPERTIES_DISABLED;

  std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);

  if (!PrepareForWrite(transaction_, edge_.ptr))
    return Error::SERIALIZATION_ERROR;

  if (edge_.ptr->deleted) return Error::DELETED_OBJECT;

  bool removed = !edge_.ptr->properties.empty();
  for (const auto &property : edge_.ptr->properties) {
    CreateAndLinkDelta(transaction_, edge_.ptr, Delta::SetPropertyTag(),
                       property.first, property.second);
  }

  edge_.ptr->properties.clear();

  return removed;
}

Result<PropertyValue> EdgeAccessor::GetProperty(PropertyId property,
                                                View view) const {
  if (!config_.properties_on_edges) return PropertyValue();
  bool exists = true;
  bool deleted = false;
  PropertyValue value;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);
    deleted = edge_.ptr->deleted;
    auto it = edge_.ptr->properties.find(property);
    if (it != edge_.ptr->properties.end()) {
      value = it->second;
    }
    delta = edge_.ptr->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view,
                     [&exists, &deleted, &value, property](const Delta &delta) {
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
  if (deleted) return Error::DELETED_OBJECT;
  return std::move(value);
}

Result<std::map<PropertyId, PropertyValue>> EdgeAccessor::Properties(
    View view) const {
  if (!config_.properties_on_edges)
    return std::map<PropertyId, PropertyValue>{};
  bool exists = true;
  bool deleted = false;
  std::map<PropertyId, PropertyValue> properties;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(edge_.ptr->lock);
    deleted = edge_.ptr->deleted;
    properties = edge_.ptr->properties;
    delta = edge_.ptr->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view,
                     [&exists, &deleted, &properties](const Delta &delta) {
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
                             properties.emplace(delta.property.key,
                                                delta.property.value);
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
  if (deleted) return Error::DELETED_OBJECT;
  return std::move(properties);
}

}  // namespace storage
