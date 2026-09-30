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

#include <array>
#include <cstdint>
#include <string_view>
#include <utility>

#include <nlohmann/json_fwd.hpp>

namespace memgraph::storage {

enum class StorageMode : std::uint8_t {
  IN_MEMORY_ANALYTICAL,
  IN_MEMORY_TRANSACTIONAL,
  ON_DISK_TRANSACTIONAL,
  /* Leave at end */ N
};

// nlohmann ADL hooks: persist StorageMode as its underlying integer (matching nlohmann's built-in
// enum default, so durable formats stay integer-encoded) but range-check the value on read via
// NumToEnum, falling back to IN_MEMORY_TRANSACTIONAL — the built-in from_json blind-casts. Defined
// in storage_mode.cpp so the full <nlohmann/json.hpp> stays out of this widely-included header.
void to_json(nlohmann::json &j, StorageMode mode);
void from_json(const nlohmann::json &j, StorageMode &mode);

inline constexpr std::array storage_mode_mappings{
    std::pair{std::string_view{"IN_MEMORY_TRANSACTIONAL"}, StorageMode::IN_MEMORY_TRANSACTIONAL},
    std::pair{std::string_view{"IN_MEMORY_ANALYTICAL"}, StorageMode::IN_MEMORY_ANALYTICAL},
    std::pair{std::string_view{"ON_DISK_TRANSACTIONAL"}, StorageMode::ON_DISK_TRANSACTIONAL}};

bool IsTransactional(StorageMode storage_mode) noexcept;

std::string_view StorageModeToString(StorageMode storage_mode);

}  // namespace memgraph::storage
