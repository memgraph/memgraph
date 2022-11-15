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

/// @file Conversion functions between Value and other memgraph types.
#pragma once

#include "communication/bolt/v1/value.hpp"
#include "coordinator/shard_map.hpp"
#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/view.hpp"

namespace memgraph::storage::v3 {
class EdgeAccessor;
class Storage;
class VertexAccessor;
}  // namespace memgraph::storage::v3

namespace memgraph::glue::v2 {

/// @param storage::v3::VertexAccessor for converting to
///        communication::bolt::Vertex.
/// @param msgs::ShardRequestManagerInterface *shard_request_manager getting label and property names.
/// @param storage::v3::View for deciding which vertex attributes are visible.
///
/// @throw std::bad_alloc
storage::v3::ShardResult<communication::bolt::Vertex> ToBoltVertex(
    const storage::v3::VertexAccessor &vertex, const msgs::ShardRequestManagerInterface *shard_request_manager,
    storage::v3::View view);

/// @param storage::v3::EdgeAccessor for converting to communication::bolt::Edge.
/// @param msgs::ShardRequestManagerInterface *shard_request_manager getting edge type and property names.
/// @param storage::v3::View for deciding which edge attributes are visible.
///
/// @throw std::bad_alloc
storage::v3::ShardResult<communication::bolt::Edge> ToBoltEdge(
    const storage::v3::EdgeAccessor &edge, const msgs::ShardRequestManagerInterface *shard_request_manager,
    storage::v3::View view);

/// @param query::v2::Path for converting to communication::bolt::Path.
/// @param msgs::ShardRequestManagerInterface *shard_request_manager ToBoltVertex and ToBoltEdge.
/// @param storage::v3::View for ToBoltVertex and ToBoltEdge.
///
/// @throw std::bad_alloc
storage::v3::ShardResult<communication::bolt::Path> ToBoltPath(
    const query::v2::accessors::Path &path, const msgs::ShardRequestManagerInterface *shard_request_manager,
    storage::v3::View view);

/// @param query::v2::TypedValue for converting to communication::bolt::Value.
/// @param msgs::ShardRequestManagerInterface *shard_request_manager ToBoltVertex and ToBoltEdge.
/// @param storage::v3::View for ToBoltVertex and ToBoltEdge.
///
/// @throw std::bad_alloc
storage::v3::ShardResult<communication::bolt::Value> ToBoltValue(
    const query::v2::TypedValue &value, const msgs::ShardRequestManagerInterface *shard_request_manager,
    storage::v3::View view);

query::v2::TypedValue ToTypedValue(const communication::bolt::Value &value);

communication::bolt::Value ToBoltValue(const storage::v3::PropertyValue &value);

storage::v3::PropertyValue ToPropertyValue(const communication::bolt::Value &value);

communication::bolt::Value ToBoltValue(msgs::Value value);

communication::bolt::Value ToBoltValue(msgs::Value value,
                                       const msgs::ShardRequestManagerInterface *shard_request_manager,
                                       storage::v3::View view);

}  // namespace memgraph::glue::v2
