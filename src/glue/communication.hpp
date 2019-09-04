/// @file Conversion functions between Value and other memgraph types.
#pragma once

#include "communication/bolt/v1/value.hpp"
#include "query/typed_value.hpp"
#include "storage/common/types/property_value.hpp"
#include "storage/v2/view.hpp"

namespace glue {

communication::bolt::Vertex ToBoltVertex(const VertexAccessor &vertex,
                                         storage::View view);

communication::bolt::Edge ToBoltEdge(const EdgeAccessor &edge,
                                     storage::View view);

communication::bolt::Path ToBoltPath(const query::Path &path,
                                     storage::View view);

communication::bolt::Value ToBoltValue(const query::TypedValue &value,
                                       storage::View view);

query::TypedValue ToTypedValue(const communication::bolt::Value &value);

communication::bolt::Value ToBoltValue(const PropertyValue &value);

PropertyValue ToPropertyValue(const communication::bolt::Value &value);

}  // namespace glue
