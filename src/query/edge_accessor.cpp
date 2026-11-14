// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/edge_accessor.hpp"

#include "query/vertex_accessor.hpp"
#ifdef MG_ENTERPRISE
#include <map>
#include <optional>
#include <utility>

#include "storage/v2/indices/active_indices.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "versioning/branch_engine.hpp"
#endif

namespace memgraph::query {

#ifndef MG_ENTERPRISE
VertexAccessor EdgeAccessor::To() const { return VertexAccessor(impl_.ToVertex()); }

VertexAccessor EdgeAccessor::From() const { return VertexAccessor(impl_.FromVertex()); }

/// When edge is deleted and you are accessing To vertex
/// for_deleted_ flag will in this case be updated properly
VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const { return VertexAccessor(impl_.DeletedEdgeToVertex()); }

/// When edge is deleted and you are accessing From vertex
/// for_deleted_ flag will in this case be updated properly
VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const { return VertexAccessor(impl_.DeletedEdgeFromVertex()); }
#endif

bool EdgeAccessor::IsCycle() const { return To() == From(); }

#ifdef MG_ENTERPRISE
VertexAccessor EdgeAccessor::BranchEndpoint(Endpoint endpoint) const {
  auto const raw = [&] {
    switch (endpoint) {
      case Endpoint::kTo:
        return impl_.ToVertex();
      case Endpoint::kFrom:
        return impl_.FromVertex();
      case Endpoint::kDeletedTo:
        return impl_.DeletedEdgeToVertex();
      case Endpoint::kDeletedFrom:
        return impl_.DeletedEdgeFromVertex();
    }
    std::unreachable();
  }();
  return VertexAccessor(branch_ctx()->ForkIdentity(raw));
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> EdgeAccessor::BranchProperties(
    storage::View view) const {
  return branch_ctx()->Properties(impl_, view);
}

storage::Result<storage::PropertyValue> EdgeAccessor::BranchGetProperty(storage::View view,
                                                                        storage::PropertyId key) const {
  return branch_ctx()->GetProperty(impl_, key, view);
}

storage::Result<uint64_t> EdgeAccessor::BranchGetPropertySize(storage::PropertyId key, storage::View view) const {
  return branch_ctx()->GetPropertySize(impl_, key, view);
}

storage::Result<void> EdgeAccessor::BranchReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                                             std::span<storage::PropertyValue> out,
                                                             storage::View view) const {
  return branch_ctx()->ReadPropertyValues(impl_, ordered_properties, out, view);
}

std::vector<storage::PropertyId> EdgeAccessor::BranchVectorIndexedProperties() const {
  if (impl_.storage_ != &branch_ctx()->diff_engine()) return impl_.VectorIndexedProperties();
  return branch_ctx()->historical().GetTransaction()->active_indices_->vector_edge_->IndexedProperties(
      impl_.EdgeType());
}

storage::Result<storage::PropertyValue> EdgeAccessor::BranchSetProperty(storage::PropertyId key,
                                                                        const storage::PropertyValue &value) {
  return branch_ctx()->SetProperty(impl_, key, value);
}

storage::Result<bool> EdgeAccessor::BranchInitProperties(
    std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  return branch_ctx()->InitProperties(impl_, properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
EdgeAccessor::BranchUpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
  return branch_ctx()->UpdateProperties(impl_, properties);
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> EdgeAccessor::BranchClearProperties() {
  return branch_ctx()->ClearProperties(impl_);
}

bool EdgeAccessor::BranchIsDeleted() const {
  // Main's IsDeleted is false for every edge when it keeps no edge properties.
  if (branch_ctx()->PropertyWritesDisabled(impl_)) return false;
  if (impl_.storage_ == &branch_ctx()->diff_engine()) return impl_.IsDeleted();
  return branch_ctx()->IsForkEdgeHidden(impl_, storage::View::NEW);
}

#endif
}  // namespace memgraph::query
