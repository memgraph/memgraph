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

#include <filesystem>
#include <variant>
#include <vector>

using RecoverySnapshot = std::filesystem::path;
using RecoveryWals = std::vector<std::filesystem::path>;
struct RecoveryCurrentWal {
  explicit RecoveryCurrentWal(const uint64_t current_wal_seq_num) : current_wal_seq_num(current_wal_seq_num) {}
  uint64_t current_wal_seq_num;
};
using RecoveryStep = std::variant<RecoverySnapshot, RecoveryWals, RecoveryCurrentWal>;
