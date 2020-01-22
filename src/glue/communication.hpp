/// @file Conversion functions between Value and other memgraph types.
#pragma once

#include "communication/bolt/v1/value.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"

#ifdef MG_SINGLE_NODE_V2
#include "storage/v2/result.hpp"

namespace storage {
class EdgeAccessor;
class Storage;
class VertexAccessor;
}  // namespace storage
#endif

namespace glue {

#ifdef MG_SINGLE_NODE_V2
/// @param storage::VertexAccessor for converting to
///        communication::bolt::Vertex.
/// @param storage::Storage for getting label and property names.
/// @param storage::View for deciding which vertex attributes are visible.
///
/// @throw std::bad_alloc
storage::Result<communication::bolt::Vertex> ToBoltVertex(
    const storage::VertexAccessor &vertex, const storage::Storage &db,
    storage::View view);

/// @param storage::EdgeAccessor for converting to communication::bolt::Edge.
/// @param storage::Storage for getting edge type and property names.
/// @param storage::View for deciding which edge attributes are visible.
///
/// @throw std::bad_alloc
storage::Result<communication::bolt::Edge> ToBoltEdge(
    const storage::EdgeAccessor &edge, const storage::Storage &db,
    storage::View view);

/// @param query::Path for converting to communication::bolt::Path.
/// @param storage::Storage for ToBoltVertex and ToBoltEdge.
/// @param storage::View for ToBoltVertex and ToBoltEdge.
///
/// @throw std::bad_alloc
storage::Result<communication::bolt::Path> ToBoltPath(
    const query::Path &path, const storage::Storage &db, storage::View view);

/// @param query::TypedValue for converting to communication::bolt::Value.
/// @param storage::Storage for ToBoltVertex and ToBoltEdge.
/// @param storage::View for ToBoltVertex and ToBoltEdge.
///
/// @throw std::bad_alloc
storage::Result<communication::bolt::Value> ToBoltValue(
    const query::TypedValue &value, const storage::Storage &db,
    storage::View view);
#else
communication::bolt::Vertex ToBoltVertex(const ::VertexAccessor &vertex,
                                         storage::View view);

communication::bolt::Edge ToBoltEdge(const ::EdgeAccessor &edge,
                                     storage::View view);

communication::bolt::Path ToBoltPath(const query::Path &path,
                                     storage::View view);

communication::bolt::Value ToBoltValue(const query::TypedValue &value,
                                       storage::View view);
#endif

query::TypedValue ToTypedValue(const communication::bolt::Value &value);

communication::bolt::Value ToBoltValue(const storage::PropertyValue &value);

storage::PropertyValue ToPropertyValue(const communication::bolt::Value &value);

}  // namespace glue
