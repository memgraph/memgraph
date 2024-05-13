// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <map>
#include <set>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::storage {

class PropertyStore {
  static_assert(std::endian::native == std::endian::little,
                "PropertyStore supports only architectures using little-endian.");

 public:
  static PropertyStore CreateFromBuffer(std::string_view buffer) {
    PropertyStore store;
    store.SetBuffer(buffer);
    return store;
  }

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

  /// Returns the size of the encoded property in bytes.
  /// Returns 0 if the property does not exist.
  /// The time complexity of this function is O(n).
  uint32_t PropertySize(PropertyId property) const;

  /// Checks whether the property `property` exists in the store. The time
  /// complexity of this function is O(n).
  bool HasProperty(PropertyId property) const;

  /// Checks whether all properties in the set `properties` exist in the store. The time
  /// complexity of this function is O(n^2).
  bool HasAllProperties(const std::set<PropertyId> &properties) const;

  /// Checks whether all property values in the vector `property_values` exist in the store. The time
  /// complexity of this function is O(n^2).
  bool HasAllPropertyValues(const std::vector<PropertyValue> &property_values) const;

  /// Extracts property values for all property ids in the set `properties`. The time
  /// complexity of this function is O(n^2).
  std::optional<std::vector<PropertyValue>> ExtractPropertyValues(const std::set<PropertyId> &properties) const;

  /// Checks whether the property `property` is equal to the specified value
  /// `value`. This function doesn't perform any memory allocations while
  /// performing the equality check. The time complexity of this function is
  /// O(n).
  bool IsPropertyEqual(PropertyId property, const PropertyValue &value) const;

  /// Returns all properties currently stored in the store. The time complexity
  /// of this function is O(n).
  /// @throw std::bad_alloc
  std::map<PropertyId, PropertyValue> Properties() const;

  /// Set a property value and return `true` if insertion took place. `false` is
  /// returned if assignment took place. The time complexity of this function is
  /// O(n).
  /// @throw std::bad_alloc
  bool SetProperty(PropertyId property, const PropertyValue &value);

  /// Init property values and return `true` if insertion took place. `false` is
  /// returned if there is any existing property in property store and insertion couldn't take place. The time
  /// complexity of this function is O(n).
  /// @throw std::bad_alloc
  bool InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties);

  /// Init property values and return `true` if insertion took place. `false` is
  /// returned if there is any existing property in property store and insertion couldn't take place. The time
  /// complexity of this function is O(n*log(n)):
  /// @throw std::bad_alloc
  bool InitProperties(std::vector<std::pair<storage::PropertyId, storage::PropertyValue>> properties);

  /// Update property values in property store with sent properties. Returns vector of changed
  /// properties. Each tuple inside vector consists of PropertyId of inserted property, together with old
  /// property (if existed or empty PropertyValue if didn't exist) and new property which was inserted.
  /// The time complexity of this function is O(n*log(n)):
  /// @throw std::bad_alloc
  std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>> UpdateProperties(
      std::map<storage::PropertyId, storage::PropertyValue> &properties);

  /// Remove all properties and return `true` if any removal took place.
  /// `false` is returned if there were no properties to remove. The time
  /// complexity of this function is O(1).
  /// @throw std::bad_alloc
  bool ClearProperties();

  /// Return property buffer as a string
  std::string StringBuffer() const;

  /// Sets buffer
  void SetBuffer(std::string_view buffer);

 private:
  template <typename TContainer>
  bool DoInitProperties(const TContainer &properties);

  uint8_t buffer_[sizeof(uint32_t) + sizeof(uint8_t *)];
};

}  // namespace memgraph::storage
