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

#include <variant>

#include "io/future.hpp"
#include "query/v2/physical/mock/context.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::v2::physical::execution {

struct NonCopyable {
  NonCopyable() = default;
  NonCopyable(const NonCopyable &) = delete;
  NonCopyable &operator=(const NonCopyable &) = delete;
  NonCopyable(NonCopyable &&) noexcept = default;
  NonCopyable &operator=(NonCopyable &&) noexcept = default;
  ~NonCopyable() = default;
};

struct Operator;

struct Status {
  bool has_more;
  std::optional<std::string> error;
};
/// NOTE: In theory status and state could be coupled together, but since STATE
/// is a variant it's easier to STATUS via generic object.

struct Once {
  Operator *op;
  bool has_more{true};
};
struct ScanAll {
  Operator *op;
  std::vector<Operator *> children;
};
struct Produce {
  Operator *op;
  std::vector<Operator *> children;
};
using VarState = std::variant<Once, ScanAll, Produce>;

struct Execution {
  Status status;
  VarState state;
};

struct Operator {
  std::string name;
  std::vector<std::shared_ptr<Operator>> children;
  std::unique_ptr<multiframe::MPMCMultiframeFCFSPool> data_pool;
  VarState state;
};

/// SINGLE THREADED EXECUTE IMPLEMENTATIONS

inline Status Execute(Once & /*unused*/) { return Status{.has_more = false}; }

inline Status Execute(ScanAll & /*unused*/) { return Status{.has_more = false}; }

inline Status Execute(Produce & /*unused*/) { return Status{.has_more = false}; }

/// ASYNC EXECUTE WRAPPERS

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define DEFINE_EXECUTE_ASYNC(state_type)                                                                            \
  inline io::Future<Execution> ExecuteAsync(                                                                        \
      mock::ExecutionContext &ctx, std::function<void()> notifier, /* NOLINTNEXTLINE(bugprone-macro-parentheses) */ \
      state_type &&state) {                                                                                         \
    auto [future, promise] = io::FuturePromisePairWithNotifications<Execution>(nullptr, std::move(notifier));       \
    auto shared_promise = std::make_shared<decltype(promise)>(std::move(promise));                                  \
    ctx.thread_pool->AddTask([state = std::move(state), promise = std::move(shared_promise)]() mutable {            \
      promise->Fill({.status = Execute(state), .state = std::move(state)});                                         \
    });                                                                                                             \
    return std::move(future);                                                                                       \
  }

DEFINE_EXECUTE_ASYNC(Once)
DEFINE_EXECUTE_ASYNC(ScanAll)
DEFINE_EXECUTE_ASYNC(Produce)

#undef DEFINE_EXECUTE_ASYNC

}  // namespace memgraph::query::v2::physical::execution
