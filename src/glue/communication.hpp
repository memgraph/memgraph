// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file Conversion functions between Value and other memgraph types.
#pragma once

#include "communication/bolt/v1/value.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::storage {
class EdgeAccessor;
class Storage;
class VertexAccessor;
}  // namespace memgraph::storage

namespace memgraph::glue {

/// @param storage::VertexAccessor for converting to
///        communication::bolt::Vertex.
/// @param storage::Storage for getting label and property names.
/// @param storage::View for deciding which vertex attributes are visible.
///
/// @throw std::bad_alloc
storage::Result<communication::bolt::Vertex> ToBoltVertex(const storage::VertexAccessor &vertex,
                                                          const storage::Storage &db, storage::View view);

/// @param storage::EdgeAccessor for converting to communication::bolt::Edge.
/// @param storage::Storage for getting edge type and property names.
/// @param storage::View for deciding which edge attributes are visible.
///
/// @throw std::bad_alloc
storage::Result<communication::bolt::Edge> ToBoltEdge(const storage::EdgeAccessor &edge, const storage::Storage &db,
                                                      storage::View view);

/// @param query::Path for converting to communication::bolt::Path.
/// @param storage::Storage for ToBoltVertex and ToBoltEdge.
/// @param storage::View for ToBoltVertex and ToBoltEdge.
///
/// @throw std::bad_alloc
storage::Result<communication::bolt::Path> ToBoltPath(const query::Path &path, const storage::Storage &db,
                                                      storage::View view);

/// @param query::Graph for converting to communication::bolt::Map.
/// @param storage::Storage for ToBoltVertex and ToBoltEdge.
/// @param storage::View for ToBoltVertex and ToBoltEdge.
///
/// @throw std::bad_alloc
storage::Result<std::map<std::string, communication::bolt::Value>> ToBoltGraph(const query::Graph &graph,
                                                                               const storage::Storage &db,
                                                                               storage::View view);

/// @param query::TypedValue for converting to communication::bolt::Value.
/// @param storage::Storage for ToBoltVertex and ToBoltEdge.
/// @param storage::View for ToBoltVertex and ToBoltEdge.
///
/// @throw std::bad_alloc
storage::Result<communication::bolt::Value> ToBoltValue(const query::TypedValue &value, const storage::Storage *db,
                                                        storage::View view);

query::TypedValue ToTypedValue(const communication::bolt::Value &value);

communication::bolt::Value ToBoltValue(const storage::PropertyValue &value);

storage::PropertyValue ToPropertyValue(const communication::bolt::Value &value);

}  // namespace memgraph::glue
