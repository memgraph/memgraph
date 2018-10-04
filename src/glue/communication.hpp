/// @file Conversion functions between Value and other memgraph types.
#pragma once

#include "communication/bolt/v1/value.hpp"
#include "query/typed_value.hpp"
#include "storage/common/property_value.hpp"

namespace glue {

communication::bolt::Vertex ToBoltVertex(const VertexAccessor &vertex);

communication::bolt::Edge ToBoltEdge(const EdgeAccessor &edge);

communication::bolt::Path ToBoltPath(const query::Path &path);

communication::bolt::Value ToBoltValue(const query::TypedValue &value);

query::TypedValue ToTypedValue(const communication::bolt::Value &value);

communication::bolt::Value ToBoltValue(const PropertyValue &value);

PropertyValue ToPropertyValue(const communication::bolt::Value &value);

}  // namespace glue
