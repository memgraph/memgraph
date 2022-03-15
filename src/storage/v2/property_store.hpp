// Copyright 2022 Memgraph Ltd.
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

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::storage {

class PropertyStore {
  static_assert(std::endian::native == std::endian::little,
                "PropertyStore supports only architectures using little-endian.");

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

  /// Remove all properties and return `true` if any removal took place.
  /// `false` is returned if there were no properties to remove. The time
  /// complexity of this function is O(1).
  /// @throw std::bad_alloc
  bool ClearProperties();

 private:
  uint8_t buffer_[sizeof(uint64_t) + sizeof(uint8_t *)];
};

}  // namespace memgraph::storage
