#include "storage/v2/vertex_accessor.hpp"

#include <memory>

#include "storage/v2/mvcc.hpp"

namespace storage {

std::optional<VertexAccessor> VertexAccessor::Create(Vertex *vertex,
                                                     Transaction *transaction,
                                                     View view) {
  bool is_visible = true;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex->lock);
    is_visible = !vertex->deleted;
    delta = vertex->delta;
  }
  ApplyDeltasForRead(transaction, delta, view,
                     [&is_visible](const Delta &delta) {
                       switch (delta.action) {
                         case Delta::Action::ADD_LABEL:
                         case Delta::Action::REMOVE_LABEL:
                         case Delta::Action::SET_PROPERTY:
                           break;
                         case Delta::Action::RECREATE_OBJECT: {
                           is_visible = true;
                           break;
                         }
                         case Delta::Action::DELETE_OBJECT: {
                           is_visible = false;
                           break;
                         }
                       }
                     });
  if (!is_visible) return std::nullopt;
  return VertexAccessor{vertex, transaction};
}

Result<bool> VertexAccessor::AddLabel(uint64_t label) {
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (vertex_->deleted) return Result<bool>{Error::DELETED_OBJECT};

  if (std::find(vertex_->labels.begin(), vertex_->labels.end(), label) !=
      vertex_->labels.end())
    return Result<bool>{false};

  CreateAndLinkDelta(transaction_, vertex_, Delta::RemoveLabelTag(), label);

  vertex_->labels.push_back(label);
  return Result<bool>{true};
}

Result<bool> VertexAccessor::RemoveLabel(uint64_t label) {
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (vertex_->deleted) return Result<bool>{Error::DELETED_OBJECT};

  auto it = std::find(vertex_->labels.begin(), vertex_->labels.end(), label);
  if (it == vertex_->labels.end()) return Result<bool>{false};

  CreateAndLinkDelta(transaction_, vertex_, Delta::AddLabelTag(), label);

  std::swap(*it, *vertex_->labels.rbegin());
  vertex_->labels.pop_back();
  return Result<bool>{true};
}

Result<bool> VertexAccessor::HasLabel(uint64_t label, View view) {
  bool deleted = false;
  bool has_label = false;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    has_label = std::find(vertex_->labels.begin(), vertex_->labels.end(),
                          label) != vertex_->labels.end();
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view,
                     [&deleted, &has_label, label](const Delta &delta) {
                       switch (delta.action) {
                         case Delta::Action::REMOVE_LABEL: {
                           if (delta.label == label) {
                             CHECK(has_label) << "Invalid database state!";
                             has_label = false;
                           }
                           break;
                         }
                         case Delta::Action::ADD_LABEL: {
                           if (delta.label == label) {
                             CHECK(!has_label) << "Invalid database state!";
                             has_label = true;
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
                         case Delta::Action::SET_PROPERTY:
                           break;
                       }
                     });
  if (deleted) return Result<bool>{Error::DELETED_OBJECT};
  return Result<bool>{has_label};
}

Result<std::vector<uint64_t>> VertexAccessor::Labels(View view) {
  bool deleted = false;
  std::vector<uint64_t> labels;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    labels = vertex_->labels;
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(
      transaction_, delta, view, [&deleted, &labels](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::REMOVE_LABEL: {
            // Remove the label because we don't see the addition.
            auto it = std::find(labels.begin(), labels.end(), delta.label);
            CHECK(it != labels.end()) << "Invalid database state!";
            std::swap(*it, *labels.rbegin());
            labels.pop_back();
            break;
          }
          case Delta::Action::ADD_LABEL: {
            // Add the label because we don't see the removal.
            auto it = std::find(labels.begin(), labels.end(), delta.label);
            CHECK(it == labels.end()) << "Invalid database state!";
            labels.push_back(delta.label);
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
          case Delta::Action::SET_PROPERTY:
            break;
        }
      });
  if (deleted) return Result<std::vector<uint64_t>>{Error::DELETED_OBJECT};
  return Result<std::vector<uint64_t>>{std::move(labels)};
}

Result<bool> VertexAccessor::SetProperty(uint64_t property,
                                         const PropertyValue &value) {
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (vertex_->deleted) return Result<bool>{Error::DELETED_OBJECT};

  auto it = vertex_->properties.find(property);
  bool existed = it != vertex_->properties.end();
  if (it != vertex_->properties.end()) {
    CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property,
                       it->second);
    if (value.IsNull()) {
      // remove the property
      vertex_->properties.erase(it);
    } else {
      // set the value
      it->second = value;
    }
  } else {
    CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property,
                       PropertyValue());
    if (!value.IsNull()) {
      vertex_->properties.emplace(property, value);
    }
  }

  return Result<bool>{existed};
}

Result<PropertyValue> VertexAccessor::GetProperty(uint64_t property,
                                                  View view) {
  bool deleted = false;
  PropertyValue value;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    auto it = vertex_->properties.find(property);
    if (it != vertex_->properties.end()) {
      value = it->second;
    }
    delta = vertex_->delta;
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
                           break;
                       }
                     });
  if (deleted) return Result<PropertyValue>{Error::DELETED_OBJECT};
  return Result<PropertyValue>{std::move(value)};
}

Result<std::unordered_map<uint64_t, PropertyValue>> VertexAccessor::Properties(
    View view) {
  std::unordered_map<uint64_t, PropertyValue> properties;
  bool deleted = false;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    properties = vertex_->properties;
    delta = vertex_->delta;
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
            break;
        }
      });
  if (deleted) {
    return Result<std::unordered_map<uint64_t, PropertyValue>>{
        Error::DELETED_OBJECT};
  }
  return Result<std::unordered_map<uint64_t, PropertyValue>>{
      std::move(properties)};
}

}  // namespace storage
