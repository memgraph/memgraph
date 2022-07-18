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

#include <compare>

#include "storage/v3/property_store.hpp"
#include "storage/v3/property_value.hpp"

namespace memgraph::storage::v3 {

class KeyStore {
 public:
  explicit KeyStore(const std::vector<PropertyValue> &key_values);

  KeyStore(const KeyStore &) = delete;
  KeyStore(KeyStore &&other) noexcept = default;
  KeyStore &operator=(const KeyStore &) = delete;
  KeyStore &operator=(KeyStore &&other) noexcept = default;

  ~KeyStore() = default;

  PropertyValue GetKey(size_t index) const;

  std::vector<PropertyValue> Keys() const;

  friend bool operator<(const KeyStore &lhs, const KeyStore &rhs) {
    // TODO(antaljanosbenjamin): also compare the schema
    return std::ranges::lexicographical_compare(
        lhs.Keys(), rhs.Keys(),
        [](const PropertyValue &lhs_value, const PropertyValue &rhs_value) { return lhs_value < rhs_value; });
  }

  friend bool operator==(const KeyStore &lhs, const KeyStore &rhs) {
    return std::ranges::equal(lhs.Keys(), rhs.Keys());
  }

  friend bool operator<(const KeyStore &lhs, const std::vector<PropertyValue> &rhs) {
    // TODO(antaljanosbenjamin): also compare the schema
    return std::ranges::lexicographical_compare(
        lhs.Keys(), rhs,
        [](const PropertyValue &lhs_value, const PropertyValue &rhs_value) { return lhs_value < rhs_value; });
  }

  friend bool operator==(const KeyStore &lhs, const std::vector<PropertyValue> &rhs) {
    return std::ranges::equal(lhs.Keys(), rhs);
  }

 private:
  PropertyStore store_;
};

}  // namespace memgraph::storage::v3
