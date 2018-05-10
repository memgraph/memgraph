#include "storage/property_value_store.hpp"

using Property = storage::Property;
using Location = storage::Location;

std::atomic<uint64_t> PropertyValueStore::disk_key_cnt_ = {0};
storage::KVStore PropertyValueStore::disk_storage_("properties");

PropertyValue PropertyValueStore::at(const Property &key) const {
  auto GetValue = [&key](const auto &props) {
    for (const auto &kv : props)
      if (kv.first == key) return kv.second;
    return PropertyValue::Null;
  };

  if (key.Location() == Location::Memory) return GetValue(props_);

  return GetValue(PropsOnDisk(std::to_string(disk_key_)));
}

void PropertyValueStore::set(const Property &key, const char *value) {
  set(key, std::string(value));
}

void PropertyValueStore::set(const Property &key, const PropertyValue &value) {
  if (value.type() == PropertyValue::Type::Null) {
    erase(key);
    return;
  }

  auto SetValue = [&key, &value](auto &props) {
    for (auto &kv : props)
      if (kv.first == key) {
        kv.second = value;
        return;
      }
    props.emplace_back(key, value);
  };

  if (key.Location() == Location::Memory) {
    SetValue(props_);
  } else {
    auto props_on_disk = PropsOnDisk(std::to_string(disk_key_));
    SetValue(props_on_disk);
    disk_storage_.Put(std::to_string(disk_key_), SerializeProps(props_on_disk));
  }
}

size_t PropertyValueStore::erase(const Property &key) {
  auto EraseKey = [&key](auto &props) {
    auto found = std::find_if(props.begin(), props.end(),
                              [&key](std::pair<Property, PropertyValue> &kv) {
                                return kv.first == key;
                              });
    if (found != props.end()) {
      props.erase(found);
      return true;
    }
    return false;
  };

  if (key.Location() == Location::Memory) return EraseKey(props_);

  auto props_on_disk = PropsOnDisk(std::to_string(disk_key_));
  if (EraseKey(props_on_disk)) {
    if (props_on_disk.empty())
      return disk_storage_.Delete(std::to_string(disk_key_));
    return disk_storage_.Put(std::to_string(disk_key_),
                             SerializeProps(props_on_disk));
  }

  return false;
}

void PropertyValueStore::clear() {
  props_.clear();
  disk_storage_.Delete(std::to_string(disk_key_));
}

/* TODO(ipaljak): replace serialize/deserialize with the real implementation.
 * Currently supporting a only one property on disk per record and that property
 * must be a string.
 * */
std::string PropertyValueStore::SerializeProps(
    const std::vector<std::pair<Property, PropertyValue>> &props) const {
  if (props.size() > 1) throw std::runtime_error("Unsupported operation");
  std::stringstream strstream;
  strstream << props[0].first.Id() << "," << props[0].second;
  return strstream.str();
}

std::vector<std::pair<Property, PropertyValue>> PropertyValueStore::Deserialize(
    const std::string &serialized_props) const {
  std::istringstream strstream(serialized_props);

  std::string s;
  std::getline(strstream, s, ',');

  uint16_t id;
  std::istringstream ss(s);
  ss >> id;

  Property key(id, Location::Disk);

  std::getline(strstream, s, ',');
  PropertyValue value(s);

  std::vector<std::pair<Property, PropertyValue>> ret;
  ret.emplace_back(key, value);

  return ret;
}

std::vector<std::pair<Property, PropertyValue>> PropertyValueStore::PropsOnDisk(
    const std::string &disk_key) const {
  auto serialized = disk_storage_.Get(disk_key);
  if (serialized) return Deserialize(disk_storage_.Get(disk_key).value());
  return {};
}
