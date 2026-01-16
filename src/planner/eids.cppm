// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

module;

#include <cstdint>
#include <functional>

export module memgraph.planner.core.eids;

export import rollbear.strong_type;

export namespace memgraph::planner::core {

using EClassId = strong::type<uint32_t, struct EClassId_, strong::regular, strong::formattable, strong::hashable,
                              strong::ordered, strong::ostreamable>;
using ENodeId = strong::type<uint32_t, struct ENodeId_, strong::regular, strong::formattable, strong::hashable,
                             strong::ordered, strong::ostreamable>;

// Provide hash_value for boost::hash ADL lookup
inline std::size_t hash_value(const EClassId &id) { return std::hash<EClassId>{}(id); }

inline std::size_t hash_value(const ENodeId &id) { return std::hash<ENodeId>{}(id); }

}  // namespace memgraph::planner::core
