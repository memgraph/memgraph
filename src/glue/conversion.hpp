/// @file Conversion functions between DecodedValue and other memgraph types.
#pragma once

#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "query/typed_value.hpp"
#include "storage/property_value.hpp"

namespace glue {

communication::bolt::DecodedVertex ToDecodedVertex(
    const VertexAccessor &vertex);

communication::bolt::DecodedEdge ToDecodedEdge(const EdgeAccessor &edge);

communication::bolt::DecodedPath ToDecodedPath(const query::Path &path);

communication::bolt::DecodedValue ToDecodedValue(
    const query::TypedValue &value);

query::TypedValue ToTypedValue(const communication::bolt::DecodedValue &value);

communication::bolt::DecodedValue ToDecodedValue(const PropertyValue &value);

PropertyValue ToPropertyValue(const communication::bolt::DecodedValue &value);

}  // namespace glue
