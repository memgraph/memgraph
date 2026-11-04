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

#include <cstdint>
#include <expected>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "versioning/branch_info.hpp"

namespace memgraph::versioning {

struct LedgerBranch {
  std::string name;
  BranchInfo info;
};

struct LedgerContents {
  uint64_t next_number{2};
  std::vector<LedgerBranch> branches;
};

// Durable registry of a database's branches: one file replaced atomically per write (temp file, fsync, rename,
// directory fsync). Layout: "MGBL <version> <payload_size> <crc32 hex>\n" then a JSON payload; Read rejects a bad
// header, size, CRC or registry. Not thread-safe: the owner serializes Write/Read.
class BranchLedger {
 public:
  static constexpr uint32_t kVersion = 1;

  explicit BranchLedger(std::filesystem::path file);

  // Refuses an invalid registry; creates the parent directory if missing. Failures before the rename leave the previous
  // ledger untouched; after it the write counts as done and a directory-fsync failure is only logged.
  std::expected<void, std::string> Write(const LedgerContents &contents) const;

  // nullopt if the file does not exist; an error message if it exists but is unreadable or corrupt.
  std::expected<std::optional<LedgerContents>, std::string> Read() const;

 private:
  std::filesystem::path file_;
};

}  // namespace memgraph::versioning
