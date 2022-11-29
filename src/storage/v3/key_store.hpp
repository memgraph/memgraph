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

#include <algorithm>
#include <compare>
#include <functional>

#include "storage/v3/id_types.hpp"
#include "storage/v3/property_store.hpp"
#include "storage/v3/property_value.hpp"

namespace memgraph::storage::v3 {

// Primary key is a collection of primary properties.
using PrimaryKey = std::vector<PropertyValue>;

class KeyStore {
 public:
  explicit KeyStore(const PrimaryKey &key_values);

  KeyStore(const KeyStore &) = delete;
  KeyStore(KeyStore &&other) noexcept = default;
  KeyStore &operator=(const KeyStore &) = delete;
  KeyStore &operator=(KeyStore &&other) noexcept = default;

  ~KeyStore() = default;

  PropertyValue GetKey(size_t index) const;

  PropertyValue GetKey(PropertyId property) const;

  PrimaryKey Keys() const;

  friend bool operator<(const KeyStore &lhs, const KeyStore &rhs) {
    return std::ranges::lexicographical_compare(lhs.Keys(), rhs.Keys(), std::less<PropertyValue>{});
  }

  friend bool operator==(const KeyStore &lhs, const KeyStore &rhs) {
    return std::ranges::equal(lhs.Keys(), rhs.Keys());
  }

  friend bool operator<(const KeyStore &lhs, const PrimaryKey &rhs) {
    return std::ranges::lexicographical_compare(lhs.Keys(), rhs, std::less<PropertyValue>{});
  }

  friend bool operator==(const KeyStore &lhs, const PrimaryKey &rhs) { return std::ranges::equal(lhs.Keys(), rhs); }

 private:
  PropertyStore store_;
};

}  // namespace memgraph::storage::v3
