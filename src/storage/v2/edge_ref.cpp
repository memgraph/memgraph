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

#include "storage/v2/edge_ref.hpp"

#include <type_traits>

namespace memgraph::storage {

static_assert(sizeof(Gid) == sizeof(Edge *), "The Gid should be the same size as an Edge *!");
static_assert(std::is_standard_layout_v<Gid>, "The Gid must have a standard layout!");
static_assert(std::is_standard_layout_v<Edge *>, "The Edge * must have a standard layout!");
static_assert(std::is_standard_layout_v<EdgeRef>, "The EdgeRef must have a standard layout!");
}  // namespace memgraph::storage
