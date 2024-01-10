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

#include <cstdint>
#include <string_view>

namespace memgraph::storage {

enum class StorageMode : std::uint8_t { IN_MEMORY_ANALYTICAL, IN_MEMORY_TRANSACTIONAL, ON_DISK_TRANSACTIONAL };

bool IsTransactional(const StorageMode storage_mode) noexcept;

std::string_view StorageModeToString(memgraph::storage::StorageMode storage_mode);

}  // namespace memgraph::storage
