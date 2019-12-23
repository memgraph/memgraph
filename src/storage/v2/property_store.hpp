#pragma once

#include <map>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

namespace storage {

class PropertyStore {
 public:
  PropertyStore();

  PropertyStore(const PropertyStore &) = delete;
  PropertyStore(PropertyStore &&other) noexcept;
  PropertyStore &operator=(const PropertyStore &) = delete;
  PropertyStore &operator=(PropertyStore &&other) noexcept;

  ~PropertyStore();

  /// Returns the currently stored value for property `property`. If the
  /// property doesn't exist a Null value is returned. The time complexity of
  /// this function is O(n).
  /// @throw std::bad_alloc
  PropertyValue GetProperty(PropertyId property) const;

  /// Checks whether the property `property` exists in the store. The time
  /// complexity of this function is O(n).
  bool HasProperty(PropertyId property) const;

  /// Returns all properties currently stored in the store. The time complexity
  /// of this function is O(n).
  /// @throw std::bad_alloc
  std::map<PropertyId, PropertyValue> Properties() const;

  /// Set a property value and return `true` if insertion took place. `false` is
  /// returned if assignment took place. The time complexity of this function is
  /// O(n).
  /// @throw std::bad_alloc
  bool SetProperty(PropertyId property, const PropertyValue &value);

  /// Remove all properties and return `true` if any removal took place.
  /// `false` is returned if there were no properties to remove. The time
  /// complexity of this function is O(1).
  /// @throw std::bad_alloc
  bool ClearProperties();

 private:
  uint8_t *data_{nullptr};
  uint64_t size_{0};
};

}  // namespace storage
