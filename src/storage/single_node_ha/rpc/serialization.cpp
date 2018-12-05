#include "storage/single_node_ha/rpc/serialization.hpp"

namespace storage {

void Save(const Label &label, capnp::Label::Builder *builder) {
  builder->setStorage(label.id_);
}

void Load(Label *label, const capnp::Label::Reader &reader) {
  label->id_ = reader.getStorage();
}

void Save(const EdgeType &edge_type, capnp::EdgeType::Builder *builder) {
  builder->setStorage(edge_type.id_);
}

void Load(EdgeType *edge_type, const capnp::EdgeType::Reader &reader) {
  edge_type->id_ = reader.getStorage();
}

void Save(const Property &property, capnp::Property::Builder *builder) {
  builder->setStorage(property.id_);
}

void Load(Property *property, const capnp::Property::Reader &reader) {
  property->id_ = reader.getStorage();
}

void SaveCapnpPropertyValue(const PropertyValue &value,
                            capnp::PropertyValue::Builder *builder) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      builder->setNullType();
      return;
    case PropertyValue::Type::Bool:
      builder->setBool(value.Value<bool>());
      return;
    case PropertyValue::Type::Int:
      builder->setInteger(value.Value<int64_t>());
      return;
    case PropertyValue::Type::Double:
      builder->setDouble(value.Value<double>());
      return;
    case PropertyValue::Type::String:
      builder->setString(value.Value<std::string>());
      return;
    case PropertyValue::Type::List: {
      const auto &values = value.Value<std::vector<PropertyValue>>();
      auto list_builder = builder->initList(values.size());
      for (size_t i = 0; i < values.size(); ++i) {
        auto value_builder = list_builder[i];
        SaveCapnpPropertyValue(values[i], &value_builder);
      }
      return;
    }
    case PropertyValue::Type::Map: {
      const auto &map = value.Value<std::map<std::string, PropertyValue>>();
      auto map_builder = builder->initMap(map.size());
      size_t i = 0;
      for (const auto &kv : map) {
        auto kv_builder = map_builder[i];
        kv_builder.setKey(kv.first);
        auto value_builder = kv_builder.initValue();
        SaveCapnpPropertyValue(kv.second, &value_builder);
        ++i;
      }
      return;
    }
  }
}

void LoadCapnpPropertyValue(const capnp::PropertyValue::Reader &reader,
                            PropertyValue *value) {
  switch (reader.which()) {
    case capnp::PropertyValue::NULL_TYPE:
      *value = PropertyValue::Null;
      return;
    case capnp::PropertyValue::BOOL:
      *value = reader.getBool();
      return;
    case capnp::PropertyValue::INTEGER:
      *value = reader.getInteger();
      return;
    case capnp::PropertyValue::DOUBLE:
      *value = reader.getDouble();
      return;
    case capnp::PropertyValue::STRING:
      *value = reader.getString().cStr();
      return;
    case capnp::PropertyValue::LIST: {
      std::vector<PropertyValue> list;
      list.reserve(reader.getList().size());
      for (const auto &value_reader : reader.getList()) {
        list.emplace_back();
        LoadCapnpPropertyValue(value_reader, &list.back());
      }
      *value = list;
      return;
    }
    case capnp::PropertyValue::MAP: {
      std::map<std::string, PropertyValue> map;
      for (const auto &kv_reader : reader.getMap()) {
        auto key = kv_reader.getKey();
        LoadCapnpPropertyValue(kv_reader.getValue(), &map[key]);
      }
      *value = map;
      return;
    }
  }
}

}  // namespace storage
