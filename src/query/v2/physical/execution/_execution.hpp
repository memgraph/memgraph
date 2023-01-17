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

#include <variant>

#include "io/future.hpp"
#include "query/v2/physical/multiframe.hpp"

namespace memgraph::query::v2::physical::execution {

/// Mostly execution data structures but also implementation of DataOperator
/// because it has to interact with the underlying data pool (multiframes).

struct DataOperator;

/// NOTE: In theory STATUS and STATE could be coupled together, but since STATE
/// is a variant it's easier to access STATUS as a regular/standalone object.
struct Status {
  bool has_more;
  std::optional<std::string> error;
};

/// Stats is an object to store all statistics about the execution of a single
/// operator. Each DataOperator owns a separated Stats object. This is the
/// place to add any counters or any objects to collect statistics about the
/// execution.
struct Stats {
  uint64_t execute_calls{0};
  uint64_t processed_frames{0};
};

/// STATE objects
///
/// An alternative name would be CURSOR objects
/// Each State object holds:
///   * a pointer to it's DataOperator to store data
///   * pointers to it's child DataOperators to pull/push data
///   * the actual execution state
///   * any additional info to manage the results
struct CreateVertices {
  DataOperator *op;
  std::vector<DataOperator *> children;
};

struct Once {
  DataOperator *op;
  bool has_more{true};
};

struct Produce {
  DataOperator *op;
  std::vector<DataOperator *> children;
};

struct ScanAll {
  DataOperator *op;
  std::vector<DataOperator *> children;
  int scan_all_elems;
  std::vector<mock::Frame> data;
  std::vector<mock::Frame>::iterator data_it{data.end()};
};

struct Unwind {
  DataOperator *op;
  std::vector<DataOperator *> children;
};

using VarState = std::variant<CreateVertices, Once, Produce, ScanAll, Unwind>;

/// Keeps Status and a State close.
struct Execution {
  Status status;
  VarState state;
};

/// A tree structure, owns:
///   * children operators
///   * data pools
///   * stats how much data has been processed
///   * single instance of the Execution/State object
///   * an info what to do with the results
struct DataOperator {
  using TDataPool = multiframe::MPMCMultiframeFCFSPool;

  std::string name;
  std::vector<std::shared_ptr<DataOperator>> children;
  std::unique_ptr<TDataPool> data_pool;
  // NOTES:
  //   * State actually holds info about the operator type
  //   * At the moment CallAsync depends on the state here
  //   * The State here is also used in the sync execution
  //
  // To be 100% sure in the compile time what is this operator about -> could
  // be used to execute a query in a single threaded way or even parallelized
  // but without intra-operator parallelism because multiple are required.
  Execution execution;
  std::optional<typename TDataPool::Token> current_token_;
  Stats stats;

  ////// DATA POOL HANDLING
  /// Get/Push data from/to surrounding operators.
  std::optional<typename TDataPool::Token> NextRead() const { return data_pool->GetFull(); }
  void PassBackRead(const typename TDataPool::Token &token) const { data_pool->ReturnEmpty(token.id); }
  std::optional<typename TDataPool::Token> NextWrite() const { return data_pool->GetEmpty(); }
  void PassBackWrite(const typename TDataPool::Token &token) const { data_pool->ReturnFull(token.id); }

  template <typename TTuple>
  bool Emit(const TTuple &tuple) {
    if (!current_token_) {
      current_token_ = NextWrite();
      if (!current_token_) {
        return false;
      }
    }
    // It might be the case that previous Emit just put a frame at the last
    // available spot, but that's covered in the next if condition. In other
    // words, because of the logic above and below, the multiframe in the next
    // line will have at least one empty spot for the frame.
    current_token_->multiframe->PushBack(tuple);
    // TODO(gitbuda): Remove any potential copies from here.
    if (current_token_->multiframe->IsFull()) {
      CloseEmit();
    }
    return true;
  }

  /// An additional function is required because sometimes, e.g., during
  /// for-range loop we don't know if there is more elements -> an easy solution
  /// is to expose an additional method.
  void CloseEmit() {
    if (!current_token_) {
      return;
    }
    PassBackWrite(*current_token_);
    current_token_ = std::nullopt;
  }

  bool IsWriterDone() const { return data_pool->IsWriterDone(); }
  void MarkWriterDone() const { data_pool->MarkWriterDone(); }
  bool IsExhausted() const { return data_pool->IsExhausted(); }
  void MarkExhausted() const { data_pool->MarkExhausted(); }
  bool IsEmpty() const { return data_pool->IsEmpty(); }
  bool HasInputData(int index = 0) {
    if (children.empty()) {
      return false;
    }
    return !children.at(index)->IsEmpty();
  }
  ////// DATA POOL HANDLING
};

struct WorkOperator {
  // Read/Write data from/to the underlying data operator. It's always only
  // one.
  DataOperator *data;
  // TODO(gitbuda): WorkOperator::states not used (intra-operator parallelization).
  // All State objects should be of the same type.
  // If states.empty()     -> DataOperator holds the state
  // If states.size() == 1 -> only inter operator parallelism possible
  // If states.size() >= 2 -> inter and intra operator parallelism possible
  std::vector<Execution> executions;
};

struct ExecutionPlan {
  // ops stores info in which order operators should be executed
  // parallelism is achievable by storing more executions inside the
  // WorkOperator + implementing a valid Executor
  std::vector<WorkOperator> ops;
  // stores future object for each execution
  std::unordered_map<size_t, io::Future<Status>> f_execs;
  // additional statistical info
  uint64_t top_level_tuples{0};
};

}  // namespace memgraph::query::v2::physical::execution
