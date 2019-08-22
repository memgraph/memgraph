#include "storage/common/types/slk.hpp"

namespace slk {

void Save(const PropertyValue &value, slk::Builder *builder) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      slk::Save(static_cast<uint8_t>(0), builder);
      return;
    case PropertyValue::Type::Bool:
      slk::Save(static_cast<uint8_t>(1), builder);
      slk::Save(value.ValueBool(), builder);
      return;
    case PropertyValue::Type::Int:
      slk::Save(static_cast<uint8_t>(2), builder);
      slk::Save(value.ValueInt(), builder);
      return;
    case PropertyValue::Type::Double:
      slk::Save(static_cast<uint8_t>(3), builder);
      slk::Save(value.ValueDouble(), builder);
      return;
    case PropertyValue::Type::String:
      slk::Save(static_cast<uint8_t>(4), builder);
      slk::Save(value.ValueString(), builder);
      return;
    case PropertyValue::Type::List: {
      slk::Save(static_cast<uint8_t>(5), builder);
      const auto &values = value.ValueList();
      size_t size = values.size();
      slk::Save(size, builder);
      for (const auto &v : values) {
        slk::Save(v, builder);
      }
      return;
    }
    case PropertyValue::Type::Map: {
      slk::Save(static_cast<uint8_t>(6), builder);
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

void Load(PropertyValue *value, slk::Reader *reader) {
  uint8_t type;
  slk::Load(&type, reader);
  switch (type) {
    case static_cast<uint8_t>(0):
      *value = PropertyValue::Null;
      return;
    case static_cast<uint8_t>(1): {
      bool v;
      slk::Load(&v, reader);
      *value = v;
      return;
    }
    case static_cast<uint8_t>(2): {
      int64_t v;
      slk::Load(&v, reader);
      *value = v;
      return;
    }
    case static_cast<uint8_t>(3): {
      double v;
      slk::Load(&v, reader);
      *value = v;
      return;
    }
    case static_cast<uint8_t>(4): {
      std::string v;
      slk::Load(&v, reader);
      *value = std::move(v);
      return;
    }
    case static_cast<uint8_t>(5): {
      size_t size;
      slk::Load(&size, reader);
      std::vector<PropertyValue> list(size);
      for (size_t i = 0; i < size; ++i) {
        slk::Load(&list[i], reader);
      }
      *value = std::move(list);
      return;
    }
    case static_cast<uint8_t>(6): {
      size_t size;
      slk::Load(&size, reader);
      std::map<std::string, PropertyValue> map;
      for (size_t i = 0; i < size; ++i) {
        std::pair<std::string, PropertyValue> kv;
        slk::Load(&kv, reader);
        map.insert(kv);
      }
      *value = std::move(map);
      return;
    }
    default:
      throw slk::SlkDecodeException("Trying to load unknown PropertyValue!");
  }
}

void Save(const PropertyValueStore &properties, slk::Builder *builder) {
  size_t size = properties.size();
  slk::Save(size, builder);
  for (const auto &kv : properties) {
    slk::Save(kv, builder);
  }
}

void Load(PropertyValueStore *properties, slk::Reader *reader) {
  properties->clear();
  size_t size;
  slk::Load(&size, reader);
  for (size_t i = 0; i < size; ++i) {
    std::pair<storage::Property, PropertyValue> kv;
    slk::Load(&kv, reader);
    properties->set(kv.first, kv.second);
  }
}

}  // namespace slk
