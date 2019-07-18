#include "storage/v2/edge_accessor.hpp"

#include <memory>

#include "storage/v2/mvcc.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace storage {

VertexAccessor EdgeAccessor::FromVertex() {
  return VertexAccessor{from_vertex_, transaction_};
}

VertexAccessor EdgeAccessor::ToVertex() {
  return VertexAccessor{to_vertex_, transaction_};
}

Result<bool> EdgeAccessor::SetProperty(uint64_t property,
                                       const PropertyValue &value) {
  std::lock_guard<utils::SpinLock> guard(edge_->lock);

  if (!PrepareForWrite(transaction_, edge_))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (edge_->deleted) return Result<bool>{Error::DELETED_OBJECT};

  auto it = edge_->properties.find(property);
  bool existed = it != edge_->properties.end();
  if (it != edge_->properties.end()) {
    CreateAndLinkDelta(transaction_, edge_, Delta::SetPropertyTag(), property,
                       it->second);
    if (value.IsNull()) {
      // remove the property
      edge_->properties.erase(it);
    } else {
      // set the value
      it->second = value;
    }
  } else {
    CreateAndLinkDelta(transaction_, edge_, Delta::SetPropertyTag(), property,
                       PropertyValue());
    if (!value.IsNull()) {
      edge_->properties.emplace(property, value);
    }
  }

  return Result<bool>{existed};
}

Result<PropertyValue> EdgeAccessor::GetProperty(uint64_t property, View view) {
  bool deleted = false;
  PropertyValue value;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(edge_->lock);
    deleted = edge_->deleted;
    auto it = edge_->properties.find(property);
    if (it != edge_->properties.end()) {
      value = it->second;
    }
    delta = edge_->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view,
                     [&deleted, &value, property](const Delta &delta) {
                       switch (delta.action) {
                         case Delta::Action::SET_PROPERTY: {
                           if (delta.property.key == property) {
                             value = delta.property.value;
                           }
                           break;
                         }
                         case Delta::Action::DELETE_OBJECT: {
                           LOG(FATAL) << "Invalid accessor!";
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
  if (deleted) return Result<PropertyValue>{Error::DELETED_OBJECT};
  return Result<PropertyValue>{std::move(value)};
}

Result<std::map<uint64_t, PropertyValue>> EdgeAccessor::Properties(View view) {
  std::map<uint64_t, PropertyValue> properties;
  bool deleted = false;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(edge_->lock);
    deleted = edge_->deleted;
    properties = edge_->properties;
    delta = edge_->delta;
  }
  ApplyDeltasForRead(
      transaction_, delta, view, [&deleted, &properties](const Delta &delta) {
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
            LOG(FATAL) << "Invalid accessor!";
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
  if (deleted) {
    return Result<std::map<uint64_t, PropertyValue>>{Error::DELETED_OBJECT};
  }
  return Result<std::map<uint64_t, PropertyValue>>{std::move(properties)};
}

}  // namespace storage
