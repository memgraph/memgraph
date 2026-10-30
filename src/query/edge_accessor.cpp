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

#include "versioning/branch_engine.hpp"
#endif

namespace memgraph::query {

bool EdgeAccessor::IsCycle() const { return To() == From(); }

#ifdef MG_ENTERPRISE
// A branch-native edge on a fork vertex has the vertex's diff-engine stub as its endpoint; the query layer holds the
// fork vertex instead.
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

// A native edge is the diff engine's own object, so its flag says; a fork edge is deleted when the branch hid it.
bool EdgeAccessor::BranchIsDeleted() const {
  if (impl_.storage_ == &branch_ctx()->diff_engine()) return impl_.IsDeleted();
  return branch_ctx()->IsForkEdgeHidden(impl_, storage::View::NEW);
}

#endif
}  // namespace memgraph::query
