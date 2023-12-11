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

#include "storage/v2/storage.hpp"
#include "utils/resource_lock.hpp"

namespace memgraph::dbms {
struct SystemTransaction {
  struct Delta {
    enum class Action {
      CREATE_DATABASE,
    };

    static constexpr struct CreateDatabase {
    } create_database;

    Delta(CreateDatabase /*tag*/, storage::SalientConfig config)
        : action(Action::CREATE_DATABASE), config(std::move(config)) {}

    Delta(const Delta &) = delete;
    Delta(Delta &&) = delete;
    Delta &operator=(const Delta &) = delete;
    Delta &operator=(Delta &&) = delete;

    ~Delta() {
      switch (action) {
        case Action::CREATE_DATABASE:
          break;
          // Some deltas might have special destructor handling
      }
    }

    Action action;
    union {
      storage::SalientConfig config;
    };
  };

  explicit SystemTransaction(uint64_t timestamp) : system_timestamp(timestamp) {}

  // Currently system transitions support a single delta
  std::optional<Delta> delta{};
  uint64_t system_timestamp;
};
}  // namespace memgraph::dbms
