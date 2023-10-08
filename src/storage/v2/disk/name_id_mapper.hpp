// Copyright 2023 Memgraph Ltd.
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
#include <charconv>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

#include "kvstore/kvstore.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/name_id_mapper.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::storage {

/// Implements class adapter. Object adapters are usually better but here we need access to protected members
/// of base class and we don't want to make them public. Also, from the performance perspective, it doesn't matter
/// since we either have dynamic virtual dispatch here or on storage level.
class DiskNameIdMapper final : public NameIdMapper {
 public:
  explicit DiskNameIdMapper(std::filesystem::path name_to_id_path, std::filesystem::path id_to_name_path)
      : name_to_id_storage_(std::make_unique<kvstore::KVStore>(name_to_id_path)),
        id_to_name_storage_(std::make_unique<kvstore::KVStore>(id_to_name_path)) {
    InitializeFromDisk();
  }

  uint64_t NameToId(const std::string_view name) override {
    if (auto maybe_id = MaybeNameToId(name); maybe_id.has_value()) {
      return maybe_id.value();
    }
    uint64_t res_id = 0;
    if (auto maybe_id_from_disk = name_to_id_storage_->Get(std::string(name)); maybe_id_from_disk.has_value()) {
      auto id_disk_value = maybe_id_from_disk.value();
      res_id = utils::ParseStringToUint64(id_disk_value);
      InsertNameIdEntryToCache(std::string(name), res_id);
      InsertIdNameEntryToCache(res_id, std::string(name));
    } else {
      res_id = NameIdMapper::NameToId(name);
      MG_ASSERT(id_to_name_storage_->Put(std::to_string(res_id), std::string(name)),
                "Failed to store id to name to disk!");
      MG_ASSERT(name_to_id_storage_->Put(std::string(name), std::to_string(res_id)),
                "Failed to store id to name to disk!");
    }

    return res_id;
  }

  const std::string &IdToName(uint64_t id) override {
    auto maybe_name = NameIdMapper::MaybeIdToName(id);
    if (maybe_name.has_value()) {
      return maybe_name.value();
    }

    auto maybe_name_from_disk = id_to_name_storage_->Get(std::to_string(id));
    MG_ASSERT(maybe_name_from_disk.has_value(), "Trying to get a name from disk for an invalid ID!");

    InsertIdNameEntryToCache(id, maybe_name_from_disk.value());
    return InsertNameIdEntryToCache(maybe_name_from_disk.value(), id);
  }

 private:
  std::optional<std::reference_wrapper<const uint64_t>> MaybeNameToId(const std::string_view name) const {
    auto name_to_id_acc = name_to_id_.access();
    auto result = name_to_id_acc.find(name);
    if (result == name_to_id_acc.end()) {
      return std::nullopt;
    }
    return result->id;
  }

  const std::string &InsertNameIdEntryToCache(const std::string &name, uint64_t id) {
    auto name_to_id_acc = name_to_id_.access();
    return name_to_id_acc.insert({std::string(name), id}).first->name;
  }

  const std::string &InsertIdNameEntryToCache(uint64_t id, const std::string &name) {
    auto id_to_name_acc = id_to_name_.access();
    return id_to_name_acc.insert({id, std::string(name)}).first->name;
  }

  void InitializeFromDisk() {
    for (auto itr = name_to_id_storage_->begin(); itr != name_to_id_storage_->end(); ++itr) {
      std::string name = itr->first;
      uint64_t id = utils::ParseStringToUint64(itr->second);
      InsertNameIdEntryToCache(name, id);
      counter_.fetch_add(1, std::memory_order_release);
    }
    for (auto itr = id_to_name_storage_->begin(); itr != id_to_name_storage_->end(); ++itr) {
      uint64_t id = utils::ParseStringToUint64(itr->first);
      std::string name = itr->second;
      InsertIdNameEntryToCache(id, name);
    }
  }

  std::unique_ptr<kvstore::KVStore> name_to_id_storage_;
  std::unique_ptr<kvstore::KVStore> id_to_name_storage_;
};

}  // namespace memgraph::storage
