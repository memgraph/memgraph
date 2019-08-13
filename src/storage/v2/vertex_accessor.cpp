#include "storage/v2/vertex_accessor.hpp"

#include <memory>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/mvcc.hpp"

namespace storage {

std::optional<VertexAccessor> VertexAccessor::Create(Vertex *vertex,
                                                     Transaction *transaction,
                                                     Indices *indices,
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
                         case Delta::Action::ADD_IN_EDGE:
                         case Delta::Action::ADD_OUT_EDGE:
                         case Delta::Action::REMOVE_IN_EDGE:
                         case Delta::Action::REMOVE_OUT_EDGE:
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
  return VertexAccessor{vertex, transaction, indices};
}

Result<bool> VertexAccessor::AddLabel(LabelId label) {
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_))
    return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  if (std::find(vertex_->labels.begin(), vertex_->labels.end(), label) !=
      vertex_->labels.end())
    return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::RemoveLabelTag(), label);

  vertex_->labels.push_back(label);

  UpdateOnAddLabel(indices_, label, vertex_, *transaction_);

  return true;
}

Result<bool> VertexAccessor::RemoveLabel(LabelId label) {
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_))
    return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  auto it = std::find(vertex_->labels.begin(), vertex_->labels.end(), label);
  if (it == vertex_->labels.end()) return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::AddLabelTag(), label);

  std::swap(*it, *vertex_->labels.rbegin());
  vertex_->labels.pop_back();
  return true;
}

Result<bool> VertexAccessor::HasLabel(LabelId label, View view) const {
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
                         case Delta::Action::ADD_IN_EDGE:
                         case Delta::Action::ADD_OUT_EDGE:
                         case Delta::Action::REMOVE_IN_EDGE:
                         case Delta::Action::REMOVE_OUT_EDGE:
                           break;
                       }
                     });
  if (deleted) return Error::DELETED_OBJECT;
  return has_label;
}

Result<std::vector<LabelId>> VertexAccessor::Labels(View view) const {
  bool deleted = false;
  std::vector<LabelId> labels;
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
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
      });
  if (deleted) return Error::DELETED_OBJECT;
  return std::move(labels);
}

Result<bool> VertexAccessor::SetProperty(PropertyId property,
                                         const PropertyValue &value) {
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_))
    return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  auto it = vertex_->properties.find(property);
  bool existed = it != vertex_->properties.end();
  // We could skip setting the value if the previous one is the same to the new
  // one. This would save some memory as a delta would not be created as well as
  // avoid copying the value. The reason we are not doing that is because the
  // current code always follows the logical pattern of "create a delta" and
  // "modify in-place". Additionally, the created delta will make other
  // transactions get a SERIALIZATION_ERROR.
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

  UpdateOnSetProperty(indices_, property, value, vertex_, *transaction_);

  return !existed;
}

Result<PropertyValue> VertexAccessor::GetProperty(PropertyId property,
                                                  View view) const {
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
                         case Delta::Action::ADD_IN_EDGE:
                         case Delta::Action::ADD_OUT_EDGE:
                         case Delta::Action::REMOVE_IN_EDGE:
                         case Delta::Action::REMOVE_OUT_EDGE:
                           break;
                       }
                     });
  if (deleted) return Error::DELETED_OBJECT;
  return std::move(value);
}

Result<std::map<PropertyId, PropertyValue>> VertexAccessor::Properties(
    View view) const {
  std::map<PropertyId, PropertyValue> properties;
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
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
      });
  if (deleted) {
    return Error::DELETED_OBJECT;
  }
  return std::move(properties);
}

Result<std::vector<EdgeAccessor>> VertexAccessor::InEdges(
    const std::vector<EdgeTypeId> &edge_types, View view) const {
  std::vector<std::tuple<EdgeTypeId, Vertex *, Edge *>> in_edges;
  bool deleted = false;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    in_edges = vertex_->in_edges;
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(
      transaction_, delta, view, [&deleted, &in_edges](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::ADD_IN_EDGE: {
            // Add the edge because we don't see the removal.
            std::tuple<EdgeTypeId, Vertex *, Edge *> link{
                delta.vertex_edge.edge_type, delta.vertex_edge.vertex,
                delta.vertex_edge.edge};
            auto it = std::find(in_edges.begin(), in_edges.end(), link);
            CHECK(it == in_edges.end()) << "Invalid database state!";
            in_edges.push_back(link);
            break;
          }
          case Delta::Action::REMOVE_IN_EDGE: {
            // Remove the label because we don't see the addition.
            std::tuple<EdgeTypeId, Vertex *, Edge *> link{
                delta.vertex_edge.edge_type, delta.vertex_edge.vertex,
                delta.vertex_edge.edge};
            auto it = std::find(in_edges.begin(), in_edges.end(), link);
            CHECK(it != in_edges.end()) << "Invalid database state!";
            std::swap(*it, *in_edges.rbegin());
            in_edges.pop_back();
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
          case Delta::Action::SET_PROPERTY:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
      });
  if (deleted) {
    return Error::DELETED_OBJECT;
  }
  std::vector<EdgeAccessor> ret;
  ret.reserve(in_edges.size());
  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    if (edge_types.empty() || std::find(edge_types.begin(), edge_types.end(),
                                        edge_type) != edge_types.end()) {
      ret.emplace_back(edge, edge_type, from_vertex, vertex_, transaction_,
                       indices_);
    }
  }
  return std::move(ret);
}

Result<std::vector<EdgeAccessor>> VertexAccessor::OutEdges(
    const std::vector<EdgeTypeId> &edge_types, View view) const {
  std::vector<std::tuple<EdgeTypeId, Vertex *, Edge *>> out_edges;
  bool deleted = false;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    out_edges = vertex_->out_edges;
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(
      transaction_, delta, view, [&deleted, &out_edges](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::ADD_OUT_EDGE: {
            // Add the edge because we don't see the removal.
            std::tuple<EdgeTypeId, Vertex *, Edge *> link{
                delta.vertex_edge.edge_type, delta.vertex_edge.vertex,
                delta.vertex_edge.edge};
            auto it = std::find(out_edges.begin(), out_edges.end(), link);
            CHECK(it == out_edges.end()) << "Invalid database state!";
            out_edges.push_back(link);
            break;
          }
          case Delta::Action::REMOVE_OUT_EDGE: {
            // Remove the label because we don't see the addition.
            std::tuple<EdgeTypeId, Vertex *, Edge *> link{
                delta.vertex_edge.edge_type, delta.vertex_edge.vertex,
                delta.vertex_edge.edge};
            auto it = std::find(out_edges.begin(), out_edges.end(), link);
            CHECK(it != out_edges.end()) << "Invalid database state!";
            std::swap(*it, *out_edges.rbegin());
            out_edges.pop_back();
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
          case Delta::Action::SET_PROPERTY:
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
            break;
        }
      });
  if (deleted) {
    return Error::DELETED_OBJECT;
  }
  std::vector<EdgeAccessor> ret;
  ret.reserve(out_edges.size());
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    if (edge_types.empty() || std::find(edge_types.begin(), edge_types.end(),
                                        edge_type) != edge_types.end()) {
      ret.emplace_back(edge, edge_type, vertex_, to_vertex, transaction_,
                       indices_);
    }
  }
  return std::move(ret);
}

}  // namespace storage
