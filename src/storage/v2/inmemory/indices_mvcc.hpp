// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/transaction_constants.hpp"

#include <atomic>

namespace memgraph::storage {

struct IndexStatus {
  IndexStatus() = default;

  bool is_populating() const { return commit_timestamp.load(std::memory_order_acquire) == kTransactionInitialId; }
  bool is_ready() const { return !is_populating(); }
  bool is_visible(uint64_t timestamp) const { return commit_timestamp.load(std::memory_order_acquire) <= timestamp; }

  void commit(uint64_t timestamp) { commit_timestamp.store(timestamp, std::memory_order_release); }

 private:
  // kTransactionInitialId means popuating
  std::atomic<uint64_t> commit_timestamp{kTransactionInitialId};
};

}  // namespace memgraph::storage
