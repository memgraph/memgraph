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

///
/// Physical Execute/Next/Emit Async Architecture Implementation
///
/// The whole new set of possibilities!
///
/// Since most of the operators have complex internal state, each Execute
/// function should be implemented in a way so that single threaded execution
/// of the whole query is possible via SingleThreadedExecutor. With the right
/// implementation, it should also be possible to parallelize execution of
/// stateless operators and simpler statefull operators like ScanAll by using
/// the same Execute implementation and MultiThreadedExecutor.
///
/// Blocking, but time and space limited, implementations of Execute functions,
/// should be wrapped into ExecuteAsync to allow efficient multi-threaded
/// execution.
///

#include <future>
#include <variant>

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

struct State {
  Operator *op;                      // access to the associated operator (data pool and other operator info)
  std::vector<Operator *> children;  // access to child operators
};

struct Once : public State {
  bool has_more{true};
};
struct ScanAll : public State {
  int cnt{0};
};
struct Produce : public State {};
using VarState = std::variant<Once, ScanAll, Produce>;

struct Execution {
  Status status;
  VarState state;
};

struct Operator {
  std::string name;
  std::vector<std::unique_ptr<Operator>> children;
  std::unique_ptr<multiframe::MPMCMultiframeFCFSPool> data_pool;
  VarState state;
};

/// SINGLE THREADED EXECUTE IMPLEMENTATIONS

inline Status Execute(Once &state) {
  state.has_more = false;
  return Status{.has_more = false};
}

inline Status Execute(ScanAll &state) {
  state.cnt++;
  if (state.cnt >= 10) {
    return Status{.has_more = false};
  }
  return Status{.has_more = true};
}

inline Status Execute(Produce & /*unused*/) { return Status{.has_more = false}; }

/// ASYNC EXECUTE WRAPPERS

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define DEFINE_EXECUTE_ASYNC(state_type)                                                                 \
  /* NOLINTNEXTLINE(bugprone-macro-parentheses) */                                                       \
  inline std::future<Execution> ExecuteAsync(mock::ExecutionContext &ctx, state_type &&state) {          \
    std::promise<Execution> promise;                                                                     \
    auto future = promise.get_future();                                                                  \
    auto shared_promise = std::make_shared<decltype(promise)>(std::move(promise));                       \
    ctx.thread_pool->AddTask([state = std::move(state), promise = std::move(shared_promise)]() mutable { \
      promise->set_value({.status = Execute(state), .state = std::move(state)});                         \
    });                                                                                                  \
    return future;                                                                                       \
  }

DEFINE_EXECUTE_ASYNC(Once)
DEFINE_EXECUTE_ASYNC(ScanAll)
DEFINE_EXECUTE_ASYNC(Produce)

#undef DEFINE_EXECUTE_ASYNC

}  // namespace memgraph::query::v2::physical::execution
