#pragma once

#include "slk/serialization.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

namespace slk {

void Save(const storage::Gid &gid, slk::Builder *builder);
void Load(storage::Gid *gid, slk::Reader *reader);

void Save(const storage::PropertyValue &value, slk::Builder *builder);
void Load(storage::PropertyValue *value, slk::Reader *reader);

void Save(const storage::durability::Marker &marker, slk::Builder *builder);
void Load(storage::durability::Marker *marker, slk::Reader *reader);

}  // namespace slk
