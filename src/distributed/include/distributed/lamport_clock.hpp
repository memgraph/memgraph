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

#include <algorithm>
#include <atomic>
#include <compare>
#include <cstdint>
#include <numeric>

namespace memgraph::distributed {

// forward declare, for strong timestamps
template <typename Tag>
struct LamportClock;

template <typename Tag>
struct timestamp {
  friend std::strong_ordering operator<=>(timestamp const &, timestamp const &) = default;

 private:
  friend struct LamportClock<Tag>;

  explicit timestamp(uint64_t value) : value_{value} {}
  uint64_t value_;
};

constexpr struct internal_t {
} internal;
constexpr struct send_t {
} send;
constexpr struct receive_t {
} receive;

template <typename Tag>
struct LamportClock {
  using timestamp_t = timestamp<Tag>;

  auto get_timestamp(internal_t) -> timestamp_t { return timestamp_t{++internal}; };
  auto get_timestamp(send_t) -> timestamp_t { return timestamp_t{++internal}; };
  auto get_timestamp(receive_t, timestamp_t received_timestamp) -> timestamp_t {
    while (true) {
      auto local_current = internal.load(std::memory_order_acquire);
      auto next = std::max(received_timestamp.value_, local_current) + 1;
      bool res = internal.compare_exchange_weak(local_current, next, std::memory_order_acq_rel);
      if (res) return timestamp_t{next};
    }
  };

 private:
  std::atomic<uint64_t> internal = 0;
};

}  // namespace memgraph::distributed
