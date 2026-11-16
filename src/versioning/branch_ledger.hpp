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
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "utils/uuid.hpp"
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

inline constexpr std::string_view kBranchLedgerKeyPrefix = "branches:";

// Durable registry of one database's branches: a single JSON value under "branches:<db-uuid>" in the DBMS durability
// KVStore. Read rejects a bad payload, version or registry. Not thread-safe: the owner serializes Write/Read.
class BranchLedger {
 public:
  static constexpr uint32_t kVersion = 1;

  BranchLedger(kvstore::KVStore &store, const utils::UUID &db_uuid);

  // Refuses an invalid registry; the write is durable (WAL synced) on success.
  std::expected<void, std::string> Write(const LedgerContents &contents) const;

  // nullopt if the database has no branches; an error message if the stored value is unreadable or invalid.
  std::expected<std::optional<LedgerContents>, std::string> Read() const;

 private:
  kvstore::KVStore *store_;
  std::string key_;
};

}  // namespace memgraph::versioning
