#include "storage/v2/replication/slk.hpp"

#include <type_traits>

#include "utils/cast.hpp"

namespace slk {

void Save(const storage::Gid &gid, slk::Builder *builder) { slk::Save(gid.AsUint(), builder); }

void Load(storage::Gid *gid, slk::Reader *reader) {
  uint64_t value;
  slk::Load(&value, reader);
  *gid = storage::Gid::FromUint(value);
}

void Save(const storage::PropertyValue::Type &type, slk::Builder *builder) {
  slk::Save(utils::UnderlyingCast(type), builder);
}

void Load(storage::PropertyValue::Type *type, slk::Reader *reader) {
  using PVTypeUnderlyingType = std::underlying_type_t<storage::PropertyValue::Type>;
  PVTypeUnderlyingType value;
  slk::Load(&value, reader);
  bool valid;
  switch (value) {
    case utils::UnderlyingCast(storage::PropertyValue::Type::Null):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Bool):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Int):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Double):
    case utils::UnderlyingCast(storage::PropertyValue::Type::String):
    case utils::UnderlyingCast(storage::PropertyValue::Type::List):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Map):
      valid = true;
      break;
    default:
      valid = false;
      break;
  }
  if (!valid) throw slk::SlkDecodeException("Trying to load unknown storage::PropertyValue!");
  *type = static_cast<storage::PropertyValue::Type>(value);
}

void Save(const storage::PropertyValue &value, slk::Builder *builder) {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      slk::Save(storage::PropertyValue::Type::Null, builder);
      return;
    case storage::PropertyValue::Type::Bool:
      slk::Save(storage::PropertyValue::Type::Bool, builder);
      slk::Save(value.ValueBool(), builder);
      return;
    case storage::PropertyValue::Type::Int:
      slk::Save(storage::PropertyValue::Type::Int, builder);
      slk::Save(value.ValueInt(), builder);
      return;
    case storage::PropertyValue::Type::Double:
      slk::Save(storage::PropertyValue::Type::Double, builder);
      slk::Save(value.ValueDouble(), builder);
      return;
    case storage::PropertyValue::Type::String:
      slk::Save(storage::PropertyValue::Type::String, builder);
      slk::Save(value.ValueString(), builder);
      return;
    case storage::PropertyValue::Type::List: {
      slk::Save(storage::PropertyValue::Type::List, builder);
      const auto &values = value.ValueList();
      size_t size = values.size();
      slk::Save(size, builder);
      for (const auto &v : values) {
        slk::Save(v, builder);
      }
      return;
    }
    case storage::PropertyValue::Type::Map: {
      slk::Save(storage::PropertyValue::Type::Map, builder);
      const auto &map = value.ValueMap();
      size_t size = map.size();
      slk::Save(size, builder);
      for (const auto &kv : map) {
        slk::Save(kv, builder);
      }
      return;
    }
  }
}

void Load(storage::PropertyValue *value, slk::Reader *reader) {
  storage::PropertyValue::Type type;
  slk::Load(&type, reader);
  switch (type) {
    case storage::PropertyValue::Type::Null:
      *value = storage::PropertyValue();
      return;
    case storage::PropertyValue::Type::Bool: {
      bool v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
    case storage::PropertyValue::Type::Int: {
      int64_t v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
    case storage::PropertyValue::Type::Double: {
      double v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
    case storage::PropertyValue::Type::String: {
      std::string v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(std::move(v));
      return;
    }
    case storage::PropertyValue::Type::List: {
      size_t size;
      slk::Load(&size, reader);
      std::vector<storage::PropertyValue> list(size);
      for (size_t i = 0; i < size; ++i) {
        slk::Load(&list[i], reader);
      }
      *value = storage::PropertyValue(std::move(list));
      return;
    }
    case storage::PropertyValue::Type::Map: {
      size_t size;
      slk::Load(&size, reader);
      std::map<std::string, storage::PropertyValue> map;
      for (size_t i = 0; i < size; ++i) {
        std::pair<std::string, storage::PropertyValue> kv;
        slk::Load(&kv, reader);
        map.insert(kv);
      }
      *value = storage::PropertyValue(std::move(map));
      return;
    }
  }
}

void Save(const storage::durability::Marker &marker, slk::Builder *builder) {
  slk::Save(utils::UnderlyingCast(marker), builder);
}

void Load(storage::durability::Marker *marker, slk::Reader *reader) {
  using PVTypeUnderlyingType = std::underlying_type_t<storage::PropertyValue::Type>;
  PVTypeUnderlyingType value;
  slk::Load(&value, reader);
  *marker = static_cast<storage::durability::Marker>(value);
}

}  // namespace slk
