#include "storage/v2/vertex_accessor.hpp"

#include <memory>

#include "storage/v2/edge_accessor.hpp"
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
                         case Delta::Action::ADD_IN_EDGE:
                         case Delta::Action::ADD_OUT_EDGE:
                         case Delta::Action::REMOVE_IN_EDGE:
                         case Delta::Action::REMOVE_OUT_EDGE:
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
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
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

Result<std::map<uint64_t, PropertyValue>> VertexAccessor::Properties(
    View view) {
  std::map<uint64_t, PropertyValue> properties;
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
    return Result<std::map<uint64_t, PropertyValue>>{Error::DELETED_OBJECT};
  }
  return Result<std::map<uint64_t, PropertyValue>>{std::move(properties)};
}

Result<std::vector<EdgeAccessor>>
VertexAccessor::InEdges(const std::vector<uint64_t> &edge_types, View view) {
  std::vector<std::tuple<uint64_t, Vertex *, Edge *>> in_edges;
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
            std::tuple<uint64_t, Vertex *, Edge *> link{
                delta.vertex_edge.edge_type, delta.vertex_edge.vertex,
                delta.vertex_edge.edge};
            auto it = std::find(in_edges.begin(), in_edges.end(), link);
            CHECK(it == in_edges.end()) << "Invalid database state!";
            in_edges.push_back(link);
            break;
          }
          case Delta::Action::REMOVE_IN_EDGE: {
            // Remove the label because we don't see the addition.
            std::tuple<uint64_t, Vertex *, Edge *> link{
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
    return Result<std::vector<EdgeAccessor>>(Error::DELETED_OBJECT);
  }
  std::vector<EdgeAccessor> ret;
  ret.reserve(in_edges.size());
  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    if (edge_types.empty() || std::find(edge_types.begin(), edge_types.end(),
                                        edge_type) != edge_types.end()) {
      ret.emplace_back(edge, edge_type, from_vertex, vertex_, transaction_);
    }
  }
  return Result<decltype(ret)>(std::move(ret));
}

Result<std::vector<EdgeAccessor>>
VertexAccessor::OutEdges(const std::vector<uint64_t> &edge_types, View view) {
  std::vector<std::tuple<uint64_t, Vertex *, Edge *>> out_edges;
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
            std::tuple<uint64_t, Vertex *, Edge *> link{
                delta.vertex_edge.edge_type, delta.vertex_edge.vertex,
                delta.vertex_edge.edge};
            auto it = std::find(out_edges.begin(), out_edges.end(), link);
            CHECK(it == out_edges.end()) << "Invalid database state!";
            out_edges.push_back(link);
            break;
          }
          case Delta::Action::REMOVE_OUT_EDGE: {
            // Remove the label because we don't see the addition.
            std::tuple<uint64_t, Vertex *, Edge *> link{
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
    return Result<std::vector<EdgeAccessor>>(Error::DELETED_OBJECT);
  }
  std::vector<EdgeAccessor> ret;
  ret.reserve(out_edges.size());
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    if (edge_types.empty() || std::find(edge_types.begin(), edge_types.end(),
                                        edge_type) != edge_types.end()) {
      ret.emplace_back(edge, edge_type, vertex_, to_vertex, transaction_);
    }
  }
  return Result<decltype(ret)>(std::move(ret));
}

}  // namespace storage
