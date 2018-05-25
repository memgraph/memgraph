#pragma once
#include <algorithm>
#include <atomic>
#include <experimental/optional>
#include <map>
#include <memory>
#include <vector>

#include "glog/logging.h"

#include "storage/kvstore_mock.hpp"
#include "storage/property_value.hpp"
#include "storage/types.hpp"
/**
 * A collection of properties accessed in a map-like way using a key of type
 * Properties::Property.
 *
 * PropertyValueStore handles storage on disk or in memory. Property key defines
 * where the corresponding property should be stored. Each instance of
 * PropertyValueStore contains a disk_key_ member which specifies where on
 * disk should the properties be stored. That key is inferred from a static
 * global counter disk_key_cnt_.
 *
 * The underlying implementation of in-memory storage is not necessarily
 * std::map.
 *
 * TODO(ipaljak) modify on-disk storage so that each property has its own
 * key for storage. Storage key should, in essence, be an ordered pair
 * (global key, property key).
 */
class PropertyValueStore {
  using Property = storage::Property;
  using Location = storage::Location;

 public:
  // Property name which will be used to store vertex/edge ids inside property
  // value store
  static constexpr char IdPropertyName[] = "__id__";

  PropertyValueStore() = default;

  PropertyValueStore(const PropertyValueStore &old) {
    // We need to update disk key and disk key counter when calling a copy
    // constructor due to mvcc.
    props_ = old.props_;
    disk_key_ = disk_key_cnt_++;
    auto old_value = disk_storage_.Get(std::to_string(old.disk_key_));
    if (old_value)
      disk_storage_.Put(std::to_string(disk_key_), old_value.value());
  }

  ~PropertyValueStore() { disk_storage_.Delete(std::to_string(disk_key_)); }

  /**
   * Returns a PropertyValue (by reference) at the given key.
   * If the key does not exist, the Null property is returned.
   *
   * This is NOT thread-safe, the reference might not be valid
   * when used in a multithreaded scenario.
   *
   * @param key The key for which a PropertyValue is sought.
   * @return  See above.
   */
  PropertyValue at(const Property &key) const;

  /**
   * Sets the value for the given key. A new PropertyValue instance
   * is created for the given value (which is of raw type). If the property
   * is to be stored on disk then that instance does not represent an additional
   * memory overhead as it goes out of scope at the end of this method.
   *
   * @tparam TValue Type of value. It must be one of the
   * predefined possible PropertyValue values (bool, string, int...)
   * @param key  The key for which the property is set. The previous
   * value at the same key (if there was one) is replaced.
   * @param value  The value to set.
   */
  template <typename TValue>
  void set(const Property &key, const TValue &value) {
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
      disk_storage_.Put(std::to_string(disk_key_),
                        SerializeProps(props_on_disk));
    }
  }

  /**
   * Set overriding for character constants. Forces conversion
   * to std::string, otherwise templating might cast the pointer
   * to something else (bool) and mess things up.
   */
  void set(const Property &key, const char *value);

  /**
   * Set overriding for PropertyValue. When setting a Null value it
   * calls 'erase' instead of inserting the Null into storage.
   */
  void set(const Property &key, const PropertyValue &value);

  /**
   * Removes the PropertyValue for the given key.
   *
   * @param key The key for which to remove the property.
   * @return  The number of removed properties (0 or 1).
   */
  size_t erase(const Property &key);

  /** Removes all the properties (both in-mem and on-disk) from this store. */
  void clear();

  /**
   * Custom PVS iterator behaves as if all properties are stored in a single
   * iterable collection of std::pair<Property, PropertyValue>.
   * */
  class iterator : public std::iterator<
                       std::input_iterator_tag,             // iterator_category
                       std::pair<Property, PropertyValue>,  // value_type
                       long,                                // difference_type
                       const std::pair<Property, PropertyValue> *,  // pointer
                       const std::pair<Property, PropertyValue> &   // reference
                       > {
   public:
    explicit iterator(const PropertyValueStore *init, int it)
        : PVS_(init), it_(it) {}

    iterator &operator++() {
      ++it_;
      return *this;
    }

    iterator operator++(int) {
      iterator ret = *this;
      ++(*this);
      return ret;
    }

    bool operator==(const iterator &other) const {
      return PVS_ == other.PVS_ && it_ == other.it_;
    }

    bool operator!=(const iterator &other) const {
      return PVS_ != other.PVS_ || it_ != other.it_;
    }

    reference operator*() {
      if (it_ < static_cast<int>(PVS_->props_.size())) return PVS_->props_[it_];
      auto disk_props = PVS_->PropsOnDisk(std::to_string(PVS_->disk_key_));
      disk_prop_ = disk_props[it_ - PVS_->props_.size()];
      return disk_prop_.value();
    }

    pointer operator->() { return &**this; }

   private:
    const PropertyValueStore *PVS_;
    int32_t it_;
    std::experimental::optional<std::pair<Property, PropertyValue>> disk_prop_;
  };

  size_t size() const {
    size_t ram_size = props_.size();
    size_t disk_size = PropsOnDisk(std::to_string(disk_key_)).size();
    return ram_size + disk_size;
  }

  auto begin() const { return iterator(this, 0); }
  auto end() const { return iterator(this, size()); }

 private:
  static std::atomic<uint64_t> disk_key_cnt_;
  uint64_t disk_key_ = disk_key_cnt_++;

  static storage::KVStore disk_storage_;

  std::vector<std::pair<Property, PropertyValue>> props_;

  std::string SerializeProps(
      const std::vector<std::pair<Property, PropertyValue>> &props) const;

  std::vector<std::pair<Property, PropertyValue>> Deserialize(
      const std::string &serialized_props) const;

  std::vector<std::pair<Property, PropertyValue>> PropsOnDisk(
      const std::string &disk_key) const;
};
