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

#include <iterator>
#include <ranges>

#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"

namespace memgraph::storage::v3 {

KeyStore::KeyStore(const std::vector<PropertyValue> &key_values) {
  for (auto i = 0; i < key_values.size(); ++i) {
    store_.SetProperty(PropertyId::FromInt(i), key_values[i]);
  }
}

PropertyValue KeyStore::GetKey(const size_t index) const { return store_.GetProperty(PropertyId::FromUint(index)); }

std::vector<PropertyValue> KeyStore::Keys() const {
  auto keys_map = store_.Properties();
  std::vector<PropertyValue> keys;
  keys.reserve(keys_map.size());
  std::ranges::transform(
      keys_map, std::back_inserter(keys),
      [](std::pair<const PropertyId, PropertyValue> &id_and_value) { return std::move(id_and_value.second); });
  return keys;
}

}  // namespace memgraph::storage::v3
