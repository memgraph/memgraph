// Copyright 2026 Memgraph Ltd.
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
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_property_index_entry.hpp"

namespace memgraph::versioning {

// Keys of the mirrored label+property indexes: a label plus root properties (a nested path reduces to its root).
// A stub holds only its own labels and properties, so a write moving one part of a key needs the rest copied on.
class IndexKeyParts {
 public:
  struct Key {
    storage::LabelId label;
    std::vector<storage::PropertyId> properties;  // roots, index order, distinct
  };

  IndexKeyParts() = default;

  explicit IndexKeyParts(const std::vector<storage::LabelPropertyIndexEntry> &indexes) {
    for (auto const &index : indexes) {
      Key key{.label = index.label, .properties = {}};
      for (auto const &path : index.properties) {
        if (path.empty()) continue;
        if (!std::ranges::contains(key.properties, path.front())) key.properties.push_back(path.front());
      }
      if (key.properties.empty()) continue;
      auto const same = [&](const Key &other) {
        return other.label == key.label && other.properties == key.properties;
      };
      // The same key under both orders is one key.
      if (std::ranges::any_of(keys_, same)) continue;
      auto const position = static_cast<uint32_t>(keys_.size());
      by_label_[key.label].push_back(position);
      for (auto const property : key.properties) by_property_[property].push_back(position);
      keys_.push_back(std::move(key));
    }
  }

  bool empty() const noexcept { return keys_.empty(); }

  const std::vector<Key> &keys() const noexcept { return keys_; }

  // Calls `visit(const Key &)` for each key `label` is a part of.
  template <typename Visit>
  void ForEachKeyOfLabel(storage::LabelId label, Visit &&visit) const {
    if (auto const it = by_label_.find(label); it != by_label_.end()) {
      for (auto const position : it->second) visit(keys_[position]);
    }
  }

  // Calls `visit(const Key &)` for each key `property` is a part of.
  template <typename Visit>
  void ForEachKeyOfProperty(storage::PropertyId property, Visit &&visit) const {
    if (auto const it = by_property_.find(property); it != by_property_.end()) {
      for (auto const position : it->second) visit(keys_[position]);
    }
  }

 private:
  std::vector<Key> keys_;
  std::unordered_map<storage::LabelId, std::vector<uint32_t>> by_label_;
  std::unordered_map<storage::PropertyId, std::vector<uint32_t>> by_property_;
};

}  // namespace memgraph::versioning
