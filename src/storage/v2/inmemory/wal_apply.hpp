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
#include <functional>
#include <utility>

#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/wal.hpp"

namespace memgraph::storage {

enum class WalApplyMode : uint8_t {
  kStream,          // transaction streamed from main, committed once its end delta arrives
  kStreamTwoPhase,  // as kStream, but the accessor is returned unreset so the caller can finalize or abort it
  kWalFile,         // transaction read from a WAL file: CRC-verified, committed immediately, uncommitted ones skipped
};

struct WalApplyOptions {
  WalApplyMode mode{WalApplyMode::kStream};
  // Called before each delta, and from long-running index/constraint builds and the commit.
  std::function<void()> on_progress;
  // Polled by long-running index/constraint builds; returning true abandons the build.
  std::function<bool()> should_cancel;
  // Called once per transaction with its durable timestamp, before its first delta is applied. Skipped
  // (already applied) transactions are not reported.
  std::function<void(uint64_t)> on_txn_begin;
};

// Reads one WAL delta. Throws utils::BasicException on truncated or invalid data.
std::pair<uint64_t, durability::WalDeltaData> ReadWalDelta(durability::BaseDecoder *decoder, uint64_t version);

}  // namespace memgraph::storage
