// Copyright 2021 Memgraph Ltd.
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

#include <atomic>
#include <string>
#include <string_view>

#include "utils/logging.hpp"
#include "utils/skip_list.hpp"

namespace storage {

class NameIdMapper final {
 private:
  struct MapNameToId {
    std::string name;
    uint64_t id;

    bool operator<(const MapNameToId &other) { return name < other.name; }
    bool operator==(const MapNameToId &other) { return name == other.name; }

    bool operator<(const std::string_view &other) { return name < other; }
    bool operator==(const std::string_view &other) { return name == other; }
  };

  struct MapIdToName {
    uint64_t id;
    std::string name;

    bool operator<(const MapIdToName &other) { return id < other.id; }
    bool operator==(const MapIdToName &other) { return id == other.id; }

    bool operator<(uint64_t other) { return id < other; }
    bool operator==(uint64_t other) { return id == other; }
  };

 public:
  /// @throw std::bad_alloc if unable to insert a new mapping
  uint64_t NameToId(const std::string_view &name) {
    auto name_to_id_acc = name_to_id_.access();
    auto found = name_to_id_acc.find(name);
    uint64_t id;
    if (found == name_to_id_acc.end()) {
      uint64_t new_id = counter_.fetch_add(1, std::memory_order_acq_rel);
      // Try to insert the mapping with the `new_id`, but use the id that is in
      // the object itself. The object that cointains the mapping is designed to
      // be a map, so that if the inserted name already exists `insert` will
      // return an iterator to the existing item. This prevents assignment of
      // two IDs to the same name when the mapping is being inserted
      // concurrently from two threads. One ID is wasted in that case, though.
      id = name_to_id_acc.insert({std::string(name), new_id}).first->id;
    } else {
      id = found->id;
    }
    auto id_to_name_acc = id_to_name_.access();
    // We have to try to insert the ID to name mapping even if we are not the
    // one who assigned the ID because we have to make sure that after this
    // method returns that both mappings exist.
    if (id_to_name_acc.find(id) == id_to_name_acc.end()) {
      // We first try to find the `id` in the map to avoid making an unnecessary
      // temporary memory allocation when the object already exists.
      id_to_name_acc.insert({id, std::string(name)});
    }
    return id;
  }

  // NOTE: Currently this function returns a `const std::string &` instead of a
  // `std::string` to avoid making unnecessary copies of the string.
  // Usually, this wouldn't be correct because the accessor to the
  // `utils::SkipList` is destroyed in this function and that removes the
  // guarantee that the reference to the value contained in the list will be
  // valid.
  // Currently, we never delete anything from the `utils::SkipList` so the
  // references will always be valid. If you change this class to remove unused
  // names, be sure to change the signature of this function.
  const std::string &IdToName(uint64_t id) const {
    auto id_to_name_acc = id_to_name_.access();
    auto result = id_to_name_acc.find(id);
    MG_ASSERT(result != id_to_name_acc.end(), "Trying to get a name for an invalid ID!");
    return result->name;
  }

 private:
  std::atomic<uint64_t> counter_{0};
  utils::SkipList<MapNameToId> name_to_id_;
  utils::SkipList<MapIdToName> id_to_name_;
};
}  // namespace storage
