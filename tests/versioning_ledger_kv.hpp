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

#include <filesystem>
#include <memory>
#include <string>
#include <utility>

#include "kvstore/kvstore.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::tests {

// The DBMS durability KVStore that holds the branch ledgers, shared by the databases of one test.
class LedgerKv {
 public:
  explicit LedgerKv(std::filesystem::path directory) : directory_{std::move(directory)} {}

  kvstore::KVStore *Get() {
    if (!kv_) kv_ = std::make_unique<kvstore::KVStore>(directory_);
    return kv_.get();
  }

  // Closes and wipes the store; no Database may be alive.
  void Reset() {
    kv_.reset();
    std::filesystem::remove_all(directory_);
  }

  static std::string Key(const utils::UUID &db_uuid) {
    return std::string{versioning::kBranchLedgerKeyPrefix} + std::string{db_uuid};
  }

 private:
  std::filesystem::path directory_;
  std::unique_ptr<kvstore::KVStore> kv_;
};

}  // namespace memgraph::tests
