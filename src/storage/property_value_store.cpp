#include <experimental/filesystem>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "storage/pod_buffer.hpp"
#include "storage/property_value_store.hpp"

namespace fs = std::experimental::filesystem;

using namespace communication::bolt;

std::atomic<uint64_t> PropertyValueStore::global_key_cnt_ = {0};

// properties on disk are stored in a directory named properties within the
// durability directory
DECLARE_string(durability_directory);
DECLARE_string(properties_on_disk);

std::string DiskKeyPrefix(const std::string &version_key) {
  return version_key + disk_key_separator;
}

std::string DiskKey(const std::string &version_key,
                    const std::string &property_id) {
  return DiskKeyPrefix(version_key) + property_id;
}

PropertyValueStore::PropertyValueStore(const PropertyValueStore &old)
    : props_(old.props_) {
  // We need to update disk key and disk key counter when calling a copy
  // constructor due to mvcc.
  if (!FLAGS_properties_on_disk.empty()) {
    version_key_ = global_key_cnt_++;
    storage::KVStore::iterator old_disk_it(
        &DiskStorage(), DiskKeyPrefix(std::to_string(old.version_key_)));
    iterator it(&old, old.props_.end(), std::move(old_disk_it));

    while (it != old.end()) {
      this->set(it->first, it->second);
      ++it;
    }
  }
}

PropertyValueStore::~PropertyValueStore() {
  if (!FLAGS_properties_on_disk.empty())
    DiskStorage().DeletePrefix(DiskKeyPrefix(std::to_string(version_key_)));
}

PropertyValue PropertyValueStore::at(const Property &key) const {
  auto GetValue = [&key](const auto &props) {
    for (const auto &kv : props)
      if (kv.first == key) return kv.second;
    return PropertyValue::Null;
  };

  if (key.Location() == Location::Memory) return GetValue(props_);

  std::string disk_key =
      DiskKey(std::to_string(version_key_), std::to_string(key.Id()));
  auto serialized_prop = DiskStorage().Get(disk_key);
  if (serialized_prop) return DeserializeProp(serialized_prop.value());
  return PropertyValue::Null;
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
    std::string disk_key =
        DiskKey(std::to_string(version_key_), std::to_string(key.Id()));
    DiskStorage().Put(disk_key, SerializeProp(value));
  }
}

bool PropertyValueStore::erase(const Property &key) {
  auto EraseKey = [&key](auto &props) {
    auto found = std::find_if(props.begin(), props.end(),
                              [&key](std::pair<Property, PropertyValue> &kv) {
                                return kv.first == key;
                              });
    if (found != props.end()) props.erase(found);
    return true;
  };

  if (key.Location() == Location::Memory) return EraseKey(props_);

  std::string disk_key =
      DiskKey(std::to_string(version_key_), std::to_string(key.Id()));
  return DiskStorage().Delete(disk_key);
}

void PropertyValueStore::clear() {
  props_.clear();
  if (!FLAGS_properties_on_disk.empty())
    DiskStorage().DeletePrefix(DiskKeyPrefix(std::to_string(version_key_)));
}

storage::KVStore &PropertyValueStore::DiskStorage() const {
  static auto disk_storage = ConstructDiskStorage();
  return disk_storage;
}

std::string PropertyValueStore::SerializeProp(const PropertyValue &prop) const {
  storage::PODBuffer pod_buffer;
  BaseEncoder<storage::PODBuffer> encoder{pod_buffer};
  encoder.WriteTypedValue(prop);
  return std::string(reinterpret_cast<char *>(pod_buffer.buffer.data()),
                     pod_buffer.buffer.size());
}

PropertyValue PropertyValueStore::DeserializeProp(
    const std::string &serialized_prop) const {
  storage::PODBuffer pod_buffer{serialized_prop};
  Decoder<storage::PODBuffer> decoder{pod_buffer};

  DecodedValue dv;
  if (!decoder.ReadValue(&dv)) {
    DLOG(WARNING) << "Unable to read property value";
    return PropertyValue::Null;
  }
  return dv.operator PropertyValue();
}

storage::KVStore PropertyValueStore::ConstructDiskStorage() const {
  auto storage_path = fs::path() / FLAGS_durability_directory / "properties";
  if (fs::exists(storage_path)) fs::remove_all(storage_path);
  return storage::KVStore(storage_path);
}
