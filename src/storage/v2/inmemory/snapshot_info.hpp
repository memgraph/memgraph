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

#include <filesystem>
#include "utils/temporal.hpp"

namespace memgraph::storage {

struct SnapshotFileInfo {
  std::filesystem::path path;
  uint64_t start_timestamp;
  utils::LocalDateTime creation_time;
  uint64_t size;
};

}  // namespace memgraph::storage
