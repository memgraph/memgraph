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

namespace memgraph::storage {

enum class GcPhase : uint8_t { IDLE, UNLINK, INDEX_CLEANUP, DELETE };

const char *GcPhaseToString(GcPhase phase);

// Snapshot of a running GC for display in SHOW TRANSACTIONS.
struct GcRunInfoView {
  GcPhase phase;
  bool exclusive_lock;  // GC holds main_lock_ exclusively (blocks all transactions)
  bool periodic;        // periodic scheduler vs forced (FREE MEMORY / storage-mode switch)
  int64_t start_time_us;
  int64_t start_steady_ms;
};

}  // namespace memgraph::storage
