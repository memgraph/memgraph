// Copyright 2024 Memgraph Ltd.
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

namespace memgraph::query {

VertexAccessor EdgeAccessor::To() const { return VertexAccessor(impl_.ToVertex()); }

VertexAccessor EdgeAccessor::From() const { return VertexAccessor(impl_.FromVertex()); }

/// When edge is deleted and you are accessing To vertex
/// for_deleted_ flag will in this case be updated properly
VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const { return VertexAccessor(impl_.DeletedEdgeToVertex()); }

/// When edge is deleted and you are accessing From vertex
/// for_deleted_ flag will in this case be updated properly
VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const { return VertexAccessor(impl_.DeletedEdgeFromVertex()); }

bool EdgeAccessor::IsCycle() const { return To() == From(); }

}  // namespace memgraph::query
