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

#include "storage_mode.hpp"

#include <type_traits>
#include <utility>

#include <nlohmann/json.hpp>

#include "utils/enum.hpp"

namespace memgraph::storage {

bool IsTransactional(const StorageMode storage_mode) noexcept {
  return storage_mode != StorageMode::IN_MEMORY_ANALYTICAL;
}

void to_json(nlohmann::json &j, StorageMode mode) { j = std::to_underlying(mode); }

void from_json(const nlohmann::json &j, StorageMode &mode) {
  const auto raw = j.get<std::underlying_type_t<StorageMode>>();
  if (!utils::NumToEnum(raw, mode)) mode = StorageMode::IN_MEMORY_TRANSACTIONAL;
}

std::string_view StorageModeToString(StorageMode storage_mode) {
  switch (storage_mode) {
    case StorageMode::IN_MEMORY_ANALYTICAL:
      return "IN_MEMORY_ANALYTICAL";
    case StorageMode::IN_MEMORY_TRANSACTIONAL:
      return "IN_MEMORY_TRANSACTIONAL";
    case StorageMode::ON_DISK_TRANSACTIONAL:
      return "ON_DISK_TRANSACTIONAL";
    case StorageMode::N:
      std::unreachable();
  }
}

}  // namespace memgraph::storage
