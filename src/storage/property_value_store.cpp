#include <experimental/filesystem>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/conversion.hpp"
#include "storage/pod_buffer.hpp"
#include "storage/property_value_store.hpp"

namespace fs = std::experimental::filesystem;

using namespace communication::bolt;

const std::string kDiskKeySeparator = "_";

std::atomic<uint64_t> PropertyValueStore::global_key_cnt_ = {0};

// properties on disk are stored in a directory named properties within the
// durability directory
DECLARE_string(durability_directory);
DECLARE_string(properties_on_disk);

std::string DiskKeyPrefix(const std::string &version_key) {
  return version_key + kDiskKeySeparator;
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
  if (!FLAGS_properties_on_disk.empty()) {
    DiskStorage().DeletePrefix(DiskKeyPrefix(std::to_string(version_key_)));
  }
}

PropertyValue PropertyValueStore::at(const Property &key) const {
  auto GetValue = [&key](const auto &props) {
    for (const auto &kv : props)
      if (kv.first == key) return kv.second;
    return PropertyValue::Null;
  };

  if (key.Location() == Location::Memory) return GetValue(props_);

  CHECK(!FLAGS_properties_on_disk.empty()) << "Trying to read property from "
                                              "disk storage with properties on "
                                              "disk disabled!";

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
    CHECK(!FLAGS_properties_on_disk.empty()) << "Trying to read property from "
                                                "disk storage with properties "
                                                "on disk disabled!";
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

  CHECK(!FLAGS_properties_on_disk.empty()) << "Trying to read property from "
                                              "disk storage with properties on "
                                              "disk disabled!";

  std::string disk_key =
      DiskKey(std::to_string(version_key_), std::to_string(key.Id()));
  return DiskStorage().Delete(disk_key);
}

void PropertyValueStore::clear() {
  props_.clear();
  if (!FLAGS_properties_on_disk.empty()) {
    DiskStorage().DeletePrefix(DiskKeyPrefix(std::to_string(version_key_)));
  }
}

storage::KVStore &PropertyValueStore::DiskStorage() const {
  static auto disk_storage = ConstructDiskStorage();
  return disk_storage;
}

PropertyValueStore::iterator::iterator(
    const PropertyValueStore *pvs,
    std::vector<std::pair<Property, PropertyValue>>::const_iterator memory_it)
    : pvs_(pvs), memory_it_(memory_it) {}

PropertyValueStore::iterator::iterator(
    const PropertyValueStore *pvs,
    std::vector<std::pair<Property, PropertyValue>>::const_iterator memory_it,
    storage::KVStore::iterator disk_it)
    : pvs_(pvs), memory_it_(memory_it), disk_it_(std::move(disk_it)) {}

PropertyValueStore::iterator &PropertyValueStore::iterator::operator++() {
  if (memory_it_ != pvs_->props_.end()) {
    ++memory_it_;
  } else if (disk_it_) {
    ++(*disk_it_);
  }
  return *this;
}

bool PropertyValueStore::iterator::operator==(const iterator &other) const {
  return pvs_ == other.pvs_ && memory_it_ == other.memory_it_ &&
         disk_it_ == other.disk_it_;
}

bool PropertyValueStore::iterator::operator!=(const iterator &other) const {
  return !(*this == other);
}

PropertyValueStore::iterator::reference PropertyValueStore::iterator::operator
    *() {
  if (memory_it_ != pvs_->props_.end() || !disk_it_) return *memory_it_;
  std::pair<std::string, std::string> kv = *(*disk_it_);
  std::string prop_id = kv.first.substr(kv.first.find(kDiskKeySeparator) + 1);
  disk_prop_ = {Property(std::stoi(prop_id), Location::Disk),
                pvs_->DeserializeProp(kv.second)};
  return disk_prop_.value();
}

PropertyValueStore::iterator::pointer PropertyValueStore::iterator::
operator->() {
  return &**this;
}

size_t PropertyValueStore::size() const {
  if (FLAGS_properties_on_disk.empty()) {
    return props_.size();
  } else {
    return props_.size() +
           DiskStorage().Size(DiskKeyPrefix(std::to_string(version_key_)));
  }
}

PropertyValueStore::iterator PropertyValueStore::begin() const {
  if (FLAGS_properties_on_disk.empty()) {
    return iterator(this, props_.begin());
  } else {
    return iterator(
        this, props_.begin(),
        DiskStorage().begin(DiskKeyPrefix(std::to_string(version_key_))));
  }
}

PropertyValueStore::iterator PropertyValueStore::end() const {
  if (FLAGS_properties_on_disk.empty()) {
    return iterator(this, props_.end());
  } else {
    return iterator(
        this, props_.end(),
        DiskStorage().end(DiskKeyPrefix(std::to_string(version_key_))));
  }
}

std::string PropertyValueStore::SerializeProp(const PropertyValue &prop) const {
  storage::PODBuffer pod_buffer;
  BaseEncoder<storage::PODBuffer> encoder{pod_buffer};
  encoder.WriteDecodedValue(communication::ToDecodedValue(prop));
  return std::string(reinterpret_cast<char *>(pod_buffer.buffer.data()),
                     pod_buffer.buffer.size());
}

PropertyValue PropertyValueStore::DeserializeProp(
    const std::string &serialized_prop) const {
  storage::PODBuffer pod_buffer{serialized_prop};
  communication::bolt::Decoder<storage::PODBuffer> decoder{pod_buffer};

  DecodedValue dv;
  if (!decoder.ReadValue(&dv)) {
    DLOG(WARNING) << "Unable to read property value";
    return PropertyValue::Null;
  }
  return communication::ToPropertyValue(dv);
}

storage::KVStore PropertyValueStore::ConstructDiskStorage() const {
  auto storage_path = fs::path() / FLAGS_durability_directory / "properties";
  if (fs::exists(storage_path)) fs::remove_all(storage_path);
  return storage::KVStore(storage_path);
}
