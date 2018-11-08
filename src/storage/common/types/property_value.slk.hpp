#pragma once

#include "communication/rpc/serialization.hpp"
#include "communication/rpc/streams.hpp"
#include "storage/common/types/property_value.hpp"

namespace slk {
inline void Save(const PropertyValue &obj, Builder *builder) {
  switch (obj.type()) {
    case PropertyValue::Type::Null: {
      uint8_t type = 0;
      Save(type, builder);
      return;
    }
    case PropertyValue::Type::Bool: {
      uint8_t type = 1;
      Save(type, builder);
      Save(obj.Value<bool>(), builder);
      return;
    }
    case PropertyValue::Type::Int: {
      uint8_t type = 2;
      Save(type, builder);
      Save(obj.Value<int64_t>(), builder);
      return;
    }
    case PropertyValue::Type::Double: {
      uint8_t type = 3;
      Save(type, builder);
      Save(obj.Value<double>(), builder);
      return;
    }
    case PropertyValue::Type::String: {
      uint8_t type = 4;
      Save(type, builder);
      Save(obj.Value<std::string>(), builder);
      return;
    }
    case PropertyValue::Type::List: {
      uint8_t type = 5;
      Save(type, builder);
      Save(obj.Value<std::vector<PropertyValue>>(), builder);
      return;
    }
    case PropertyValue::Type::Map: {
      uint8_t type = 6;
      Save(type, builder);
      Save(obj.Value<std::map<std::string, PropertyValue>>(), builder);
      return;
    }
  }
}

inline void Load(PropertyValue *obj, Reader *reader) {
  uint8_t type = 0;
  Load(&type, reader);
  switch (type) {
    // Null
    case 0: {
      *obj = PropertyValue();
      return;
    }
    // Bool
    case 1: {
      bool value = false;
      Load(&value, reader);
      *obj = PropertyValue(value);
      return;
    }
    // Int
    case 2: {
      int64_t value = 0;
      Load(&value, reader);
      *obj = PropertyValue(value);
      return;
    }
    // Double
    case 3: {
      double value = 0.0;
      Load(&value, reader);
      *obj = PropertyValue(value);
      return;
    }
    // String
    case 4: {
      std::string value;
      Load(&value, reader);
      *obj = PropertyValue(std::move(value));
      return;
    }
    // List
    case 5: {
      std::vector<PropertyValue> value;
      Load(&value, reader);
      *obj = PropertyValue(std::move(value));
      return;
    }
    // Map
    case 6: {
      std::map<std::string, PropertyValue> value;
      Load(&value, reader);
      *obj = PropertyValue(std::move(value));
      return;
    }
    // Invalid type
    default: { throw SlkDecodeException("Couldn't load property value!"); }
  }
}
}  // namespace slk
