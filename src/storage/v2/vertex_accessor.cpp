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

#include "storage/v2/vertex_accessor.hpp"

#include <memory>

#include "query/context.hpp"
#include "query/fine_grained_access_checker.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage {

namespace detail {
namespace {
std::pair<bool, bool> IsVisible(Vertex *vertex, Transaction *transaction, View view) {
  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex->lock);
    deleted = vertex->deleted;
    delta = vertex->delta;
  }
  ApplyDeltasForRead(transaction, delta, view, [&](const Delta &delta) {
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

  return {exists, deleted};
}
}  // namespace
}  // namespace detail

std::optional<VertexAccessor> VertexAccessor::Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                                     Constraints *constraints, Config::Items config, View view) {
  if (const auto [exists, deleted] = detail::IsVisible(vertex, transaction, view); !exists || deleted) {
    return std::nullopt;
  }

  return VertexAccessor{vertex, transaction, indices, constraints, config};
}

bool VertexAccessor::IsVisible(View view) const {
  const auto [exists, deleted] = detail::IsVisible(vertex_, transaction_, view);
  return exists && (for_deleted_ || !deleted);
}

Result<bool> VertexAccessor::AddLabel(LabelId label) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  if (std::find(vertex_->labels.begin(), vertex_->labels.end(), label) != vertex_->labels.end()) return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::RemoveLabelTag(), label);

  vertex_->labels.push_back(label);

  UpdateOnAddLabel(indices_, label, vertex_, *transaction_);

  return true;
}

Result<bool> VertexAccessor::RemoveLabel(LabelId label) {
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  auto it = std::find(vertex_->labels.begin(), vertex_->labels.end(), label);
  if (it == vertex_->labels.end()) return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::AddLabelTag(), label);

  std::swap(*it, *vertex_->labels.rbegin());
  vertex_->labels.pop_back();
  return true;
}

Result<bool> VertexAccessor::HasLabel(LabelId label, View view) const {
  bool exists = true;
  bool deleted = false;
  bool has_label = false;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    has_label = std::find(vertex_->labels.begin(), vertex_->labels.end(), label) != vertex_->labels.end();
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &has_label, label](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::REMOVE_LABEL: {
        if (delta.label == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
        break;
      }
      case Delta::Action::ADD_LABEL: {
        if (delta.label == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
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
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
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
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    labels = vertex_->labels;
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &labels](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::REMOVE_LABEL: {
        // Remove the label because we don't see the addition.
        auto it = std::find(labels.begin(), labels.end(), delta.label);
        MG_ASSERT(it != labels.end(), "Invalid database state!");
        std::swap(*it, *labels.rbegin());
        labels.pop_back();
        break;
      }
      case Delta::Action::ADD_LABEL: {
        // Add the label because we don't see the removal.
        auto it = std::find(labels.begin(), labels.end(), delta.label);
        MG_ASSERT(it == labels.end(), "Invalid database state!");
        labels.push_back(delta.label);
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
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return std::move(labels);
}

Result<PropertyValue> VertexAccessor::SetProperty(PropertyId property, const PropertyValue &value) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

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

  UpdateOnSetProperty(indices_, property, value, vertex_, *transaction_);

  return std::move(current_value);
}

Result<std::map<PropertyId, PropertyValue>> VertexAccessor::ClearProperties() {
  std::lock_guard<utils::SpinLock> guard(vertex_->lock);

  if (!PrepareForWrite(transaction_, vertex_)) return Error::SERIALIZATION_ERROR;

  if (vertex_->deleted) return Error::DELETED_OBJECT;

  auto properties = vertex_->properties.Properties();
  for (const auto &property : properties) {
    CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property.first, property.second);
    UpdateOnSetProperty(indices_, property.first, PropertyValue(), vertex_, *transaction_);
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
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    value = vertex_->properties.GetProperty(property);
    delta = vertex_->delta;
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

Result<std::map<PropertyId, PropertyValue>> VertexAccessor::Properties(View view) const {
  bool exists = true;
  bool deleted = false;
  std::map<PropertyId, PropertyValue> properties;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    properties = vertex_->properties.Properties();
    delta = vertex_->delta;
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

Result<std::vector<EdgeAccessor>> VertexAccessor::InEdges(
    View view, const query::FineGrainedAccessChecker *fine_grained_access_checker,
    const VertexAccessor *destination) const {
  MG_ASSERT(!destination || destination->transaction_ == transaction_, "Invalid accessor!");
  bool exists = true;
  bool deleted = false;
  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    if (!fine_grained_access_checker && !destination) {
      in_edges = vertex_->in_edges;
    } else {
      for (const auto &item : vertex_->in_edges) {
        const auto &[edge_type, from_vertex, edge] = item;
        if (destination && from_vertex != destination->vertex_) continue;
        if (!fine_grained_access_checker && fine_grained_access_checker->IsUserAuthorizedEdgeType(edge_type) &&
            fine_grained_access_checker->IsUserAuthorizedLabels(from_vertex->labels))
          continue;
        in_edges.push_back(item);
      }
    }
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view,
                     [&exists, &deleted, &in_edges, &fine_grained_access_checker, &destination](const Delta &delta) {
                       switch (delta.action) {
                         case Delta::Action::ADD_IN_EDGE: {
                           if (destination && delta.vertex_edge.vertex != destination->vertex_) break;
                           if (!fine_grained_access_checker &&
                               fine_grained_access_checker->IsUserAuthorizedEdgeType(delta.vertex_edge.edge_type) &&
                               fine_grained_access_checker->IsUserAuthorizedLabels(delta.vertex_edge.vertex->labels))
                             break;
                           // Add the edge because we don't see the removal.
                           std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                               delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
                           auto it = std::find(in_edges.begin(), in_edges.end(), link);
                           MG_ASSERT(it == in_edges.end(), "Invalid database state!");
                           in_edges.push_back(link);
                           break;
                         }
                         case Delta::Action::REMOVE_IN_EDGE: {
                           if (destination && delta.vertex_edge.vertex != destination->vertex_) break;
                           if (!fine_grained_access_checker &&
                               fine_grained_access_checker->IsUserAuthorizedEdgeType(delta.vertex_edge.edge_type) &&
                               fine_grained_access_checker->IsUserAuthorizedLabels(delta.vertex_edge.vertex->labels))
                             break;
                           // Remove the label because we don't see the addition.
                           std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                               delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
                           auto it = std::find(in_edges.begin(), in_edges.end(), link);
                           MG_ASSERT(it != in_edges.end(), "Invalid database state!");
                           std::swap(*it, *in_edges.rbegin());
                           in_edges.pop_back();
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
                         case Delta::Action::SET_PROPERTY:
                         case Delta::Action::ADD_OUT_EDGE:
                         case Delta::Action::REMOVE_OUT_EDGE:
                           break;
                       }
                     });
  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (deleted) return Error::DELETED_OBJECT;
  std::vector<EdgeAccessor> ret;
  ret.reserve(in_edges.size());
  for (const auto &item : in_edges) {
    const auto &[edge_type, from_vertex, edge] = item;
    ret.emplace_back(edge, edge_type, from_vertex, vertex_, transaction_, indices_, constraints_, config_);
  }
  return std::move(ret);
}

Result<std::vector<EdgeAccessor>> VertexAccessor::OutEdges(
    View view, const query::FineGrainedAccessChecker *fine_grained_access_checker,
    const VertexAccessor *destination) const {
  MG_ASSERT(!destination || destination->transaction_ == transaction_, "Invalid accessor!");
  bool exists = true;
  bool deleted = false;
  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    if (!fine_grained_access_checker && !destination) {
      out_edges = vertex_->out_edges;
    } else {
      for (const auto &item : vertex_->out_edges) {
        const auto &[edge_type, to_vertex, edge] = item;
        if (destination && to_vertex != destination->vertex_) continue;
        if (!fine_grained_access_checker && fine_grained_access_checker->IsUserAuthorizedEdgeType(edge_type) &&
            fine_grained_access_checker->IsUserAuthorizedLabels(to_vertex->labels))
          continue;
        out_edges.push_back(item);
      }
    }
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view,
                     [&exists, &deleted, &out_edges, &fine_grained_access_checker, &destination](const Delta &delta) {
                       switch (delta.action) {
                         case Delta::Action::ADD_OUT_EDGE: {
                           if (destination && delta.vertex_edge.vertex != destination->vertex_) break;
                           if (!fine_grained_access_checker &&
                               fine_grained_access_checker->IsUserAuthorizedEdgeType(delta.vertex_edge.edge_type) &&
                               fine_grained_access_checker->IsUserAuthorizedLabels(delta.vertex_edge.vertex->labels))
                             break;
                           // Add the edge because we don't see the removal.
                           std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                               delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
                           auto it = std::find(out_edges.begin(), out_edges.end(), link);
                           MG_ASSERT(it == out_edges.end(), "Invalid database state!");
                           out_edges.push_back(link);
                           break;
                         }
                         case Delta::Action::REMOVE_OUT_EDGE: {
                           if (destination && delta.vertex_edge.vertex != destination->vertex_) break;
                           if (!fine_grained_access_checker &&
                               fine_grained_access_checker->IsUserAuthorizedEdgeType(delta.vertex_edge.edge_type) &&
                               fine_grained_access_checker->IsUserAuthorizedLabels(delta.vertex_edge.vertex->labels))
                             break;
                           // Remove the label because we don't see the addition.
                           std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                               delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
                           auto it = std::find(out_edges.begin(), out_edges.end(), link);
                           MG_ASSERT(it != out_edges.end(), "Invalid database state!");
                           std::swap(*it, *out_edges.rbegin());
                           out_edges.pop_back();
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
                         case Delta::Action::SET_PROPERTY:
                         case Delta::Action::ADD_IN_EDGE:
                         case Delta::Action::REMOVE_IN_EDGE:
                           break;
                       }
                     });
  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (deleted) return Error::DELETED_OBJECT;
  std::vector<EdgeAccessor> ret;
  ret.reserve(out_edges.size());
  for (const auto &item : out_edges) {
    const auto &[edge_type, to_vertex, edge] = item;
    ret.emplace_back(edge, edge_type, vertex_, to_vertex, transaction_, indices_, constraints_, config_);
  }
  return std::move(ret);
}

Result<size_t> VertexAccessor::InDegree(View view) const {
  bool exists = true;
  bool deleted = false;
  size_t degree = 0;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    degree = vertex_->in_edges.size();
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &degree](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_IN_EDGE:
        ++degree;
        break;
      case Delta::Action::REMOVE_IN_EDGE:
        --degree;
        break;
      case Delta::Action::DELETE_OBJECT:
        exists = false;
        break;
      case Delta::Action::RECREATE_OBJECT:
        deleted = false;
        break;
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
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
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);
    deleted = vertex_->deleted;
    degree = vertex_->out_edges.size();
    delta = vertex_->delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &degree](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_OUT_EDGE:
        ++degree;
        break;
      case Delta::Action::REMOVE_OUT_EDGE:
        --degree;
        break;
      case Delta::Action::DELETE_OBJECT:
        exists = false;
        break;
      case Delta::Action::RECREATE_OBJECT:
        deleted = false;
        break;
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
        break;
    }
  });
  if (!exists) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted_ && deleted) return Error::DELETED_OBJECT;
  return degree;
}

}  // namespace memgraph::storage
