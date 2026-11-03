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

#include <compare>
#include <cstdint>
#include <type_traits>

namespace memgraph::storage {

// A timestamp belongs to one of three sequences. Start and commit timestamps are drawn from the
// same counter, so a comparison between them is well-formed and orders two real points in that
// counter, which is why mixing the two has never produced a visible defect. It is still a
// comparison between answers to different questions, and a durable timestamp is a different
// counter outright on a replica. These types make each sequence its own, so a value can only be
// compared with one from the sequence it came from.
//
// Deliberately lighter than id_types.hpp: no formatting, no hashing and no string conversion, so
// that a header reachable from most of the storage engine pulls in nothing beyond the three
// standard headers above.
#define STORAGE_DEFINE_TIMESTAMP_TYPE(name)                                                  \
  class name final {                                                                         \
   private:                                                                                  \
    constexpr explicit name(uint64_t ts) : ts_{ts} {}                                        \
                                                                                             \
   public:                                                                                   \
    /* Default constructor to allow serialization or preallocation. */                       \
    name() = default;                                                                        \
                                                                                             \
    constexpr static name FromUint(uint64_t ts) { return name{ts}; }                         \
    constexpr uint64_t AsUint() const { return ts_; }                                        \
    friend constexpr bool operator==(const name &, const name &) = default;                  \
    friend constexpr std::strong_ordering operator<=>(const name &, const name &) = default; \
                                                                                             \
   private:                                                                                  \
    uint64_t ts_{};                                                                          \
  };                                                                                         \
  static_assert(std::is_trivially_copyable_v<name>, "storage::" #name " must be trivially copyable!");

// When a transaction began. Also its position in the commit log and the slot the garbage collector
// orders transactions by, which is why `OldestActive()` reports one of these.
STORAGE_DEFINE_TIMESTAMP_TYPE(StartTimestamp)

// When a transaction's writes became visible. Stamps a published delta and bounds what a snapshot
// may read.
STORAGE_DEFINE_TIMESTAMP_TYPE(CommitTimestamp)

// A position in the cluster's durability order, carried by the WAL and by replication. Equal in
// value to the commit timestamp on a main and a separate counter on a replica, so the two coincide
// exactly where it is least safe to rely on it.
STORAGE_DEFINE_TIMESTAMP_TYPE(DurableTimestamp)

#undef STORAGE_DEFINE_TIMESTAMP_TYPE

}  // namespace memgraph::storage
