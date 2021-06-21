#pragma once

#include "slk/serialization.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/concepts.hpp"

namespace slk {

void Save(const storage::Gid &gid, slk::Builder *builder);
void Load(storage::Gid *gid, slk::Reader *reader);

void Save(const storage::PropertyValue &value, slk::Builder *builder);
void Load(storage::PropertyValue *value, slk::Reader *reader);

template <utils::Enum T>
void Save(const T &enum_value, slk::Builder *builder) {
  slk::Save(utils::UnderlyingCast(enum_value), builder);
}

template <utils::Enum T>
void Load(T *enum_value, slk::Reader *reader) {
  using UnderlyingType = std::underlying_type_t<T>;
  UnderlyingType value;
  slk::Load(&value, reader);
  *enum_value = static_cast<T>(value);
}

}  // namespace slk
