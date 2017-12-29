#pragma once

#include <algorithm>
#include <map>
#include <vector>

#include "database/graph_db_datatypes.hpp"
#include "property_value.hpp"

/**
 * A collection of properties accessed in a map-like way
 * using a key of type Properties::Property.
 *
 * The underlying implementation is not necessarily std::map.
 */
class PropertyValueStore {
  using Property = GraphDbTypes::Property;

 public:
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
  const PropertyValue &at(const Property &key) const {
    for (const auto &kv : props_)
      if (kv.first == key) return kv.second;

    return PropertyValue::Null;
  }

  /**
   * Sets the value for the given key. A new PropertyValue instance
   * is created for the given value (which is of raw type).
   *
   * @tparam TValue Type of value. It must be one of the
   * predefined possible PropertyValue values (bool, string, int...)
   * @param key  The key for which the property is set. The previous
   * value at the same key (if there was one) is replaced.
   * @param value  The value to set.
   */
  template <typename TValue>
  void set(const Property &key, const TValue &value) {
    for (auto &kv : props_)
      if (kv.first == key) {
        kv.second = PropertyValue(value);
        return;
      }

    // there is no value for the given key, add new
    // TODO consider vector size increment optimization
    props_.emplace_back(key, value);
  }

  /**
   * Set overriding for character constants. Forces conversion
   * to std::string, otherwise templating might cast the pointer
   * to something else (bool) and mess things up.
   */
  void set(const Property &key, const char *value) {
    set(key, std::string(value));
  }

  /**
   * Set overriding for PropertyValue. When setting a Null value it
   * calls 'erase' instead of inserting the Null into storage.
   */
  void set(const Property &key, const PropertyValue &value) {
    if (value.type() == PropertyValue::Type::Null) {
      erase(key);
      return;
    }

    for (auto &kv : props_)
      if (kv.first == key) {
        kv.second = value;
        return;
      }

    props_.emplace_back(key, value);
  }

  /**
   * Removes the PropertyValue for the given key.
   *
   * @param key The key for which to remove the property.
   * @return  The number of removed properties (0 or 1).
   */
  size_t erase(const Property &key) {
    auto found = std::find_if(props_.begin(), props_.end(),
                              [&key](std::pair<Property, PropertyValue> &kv) {
                                return kv.first == key;
                              });

    if (found != props_.end()) {
      props_.erase(found);
      return 1;
    }
    return 0;
  }

  /** Removes all the properties from this store. */
  void clear() { props_.clear(); }

  size_t size() const { return props_.size(); }
  auto begin() const { return props_.begin(); }
  auto end() const { return props_.end(); }

 private:
  std::vector<std::pair<Property, PropertyValue>> props_;
};
