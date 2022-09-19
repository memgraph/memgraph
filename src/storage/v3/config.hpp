// Copyright 2022 Memgraph Ltd.
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

#include <chrono>
#include <cstdint>
#include <filesystem>

#include "io/time.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/isolation_level.hpp"
#include "storage/v3/transaction.hpp"

namespace memgraph::storage::v3 {

/// Pass this class to the \ref Storage constructor to change the behavior of
/// the storage. This class also defines the default behavior.
struct Config {
  struct Gc {
    // Interval after which the committed deltas are cleaned up
    io::Duration reclamation_interval{};
  } gc;

  struct Items {
    bool properties_on_edges{true};
  } items;

  struct Transaction {
    IsolationLevel isolation_level{IsolationLevel::SNAPSHOT_ISOLATION};
  } transaction;
};

}  // namespace memgraph::storage::v3
