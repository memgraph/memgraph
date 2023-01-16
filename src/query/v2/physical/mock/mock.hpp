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

#include <vector>

#include "query/v2/physical/execution.hpp"
#include "query/v2/physical/mock/frame.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "query/v2/physical/physical_ene.hpp"
#include "query/v2/physical/physical_pull.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::v2::physical::mock {

using TFrame = Frame;
using TDataPool = physical::multiframe::MPMCMultiframeFCFSPool;

using TPhysicalOperator = physical::PhysicalOperator<TDataPool>;
using TPhysicalOperatorPtr = std::shared_ptr<TPhysicalOperator>;
using TOnceOperator = physical::OncePhysicalOperator<TDataPool>;
template <typename TDataFun>
using TScanAllOperator = physical::ScanAllPhysicalOperator<TDataFun, TDataPool>;
using TProduceOperator = physical::ProducePhysicalOperator<TDataPool>;
using TExecutionContext = ExecutionContext;

using TCursor = physical::Cursor;
using TCursorPtr = std::unique_ptr<TCursor>;
using TCursorOnce = physical::OnceCursor;
template <typename TDataFun>
using TCursorScanAll = physical::ScanAllCursor<TDataFun>;
using TCursorProduce = physical::ProduceCursor;

enum class OpType { Once, ScanAll, Produce };
inline std::ostream &operator<<(std::ostream &os, const OpType &op_type) {
  switch (op_type) {
    case OpType::Once:
      os << "Once";
      break;
    case OpType::ScanAll:
      os << "ScanAll";
      break;
    case OpType::Produce:
      os << "Produce";
      break;
  }
  return os;
}

constexpr static int SCANALL_ELEMS_POS = 0;
struct Op {
  OpType type;
  std::vector<int> props;
};

inline void LogOps(const std::vector<Op> &ops) {
  for (const auto &op : ops) {
    if (op.type == OpType::ScanAll) {
      SPDLOG_INFO("{} elems: {}", op.type, op.props[SCANALL_ELEMS_POS]);
      continue;
    }
    SPDLOG_INFO("{}", op.type);
  }
}

inline TPhysicalOperatorPtr MakeENEOnce(int pool_size, int mf_size) {
  auto data_pool = std::make_unique<TDataPool>(pool_size, mf_size);
  return std::make_shared<TOnceOperator>(TOnceOperator("Physical Once", std::move(data_pool)));
}
inline TPhysicalOperatorPtr MakeENEScanAll(int pool_size, int mf_size, int scan_all_elems) {
  auto data_fun = [scan_all_elems](TDataPool::TMultiframe &mf, TExecutionContext &) {
    std::vector<TFrame> frames;
    for (int i = 0; i < scan_all_elems; ++i) {
      for (int j = 0; j < mf.Data().size(); ++j) {
        frames.push_back(TFrame{});
      }
    }
    return frames;
  };
  auto data_pool = std::make_unique<TDataPool>(pool_size, mf_size);
  return std::make_shared<TScanAllOperator<decltype(data_fun)>>(
      TScanAllOperator<decltype(data_fun)>("Physical ScanAll", std::move(data_fun), std::move(data_pool)));
}
inline TPhysicalOperatorPtr MakeENEProduce(int pool_size, int mf_size) {
  auto data_pool = std::make_unique<TDataPool>(pool_size, mf_size);
  return std::make_shared<TProduceOperator>(TProduceOperator("Physical Produce", std::move(data_pool)));
}
inline TPhysicalOperatorPtr MakeENEPlan(const std::vector<Op> &ops, int pool_size, int mf_size) {
  TPhysicalOperatorPtr plan = nullptr;
  auto current = plan;
  for (const auto &op : ops) {
    if (op.type == OpType::Once) {
      auto once = MakeENEOnce(pool_size, mf_size);
      current->AddChild(once);
      current = once;
    } else if (op.type == OpType::ScanAll) {
      auto scan_all = MakeENEScanAll(pool_size, mf_size, op.props[SCANALL_ELEMS_POS]);
      current->AddChild(scan_all);
      current = scan_all;
    } else if (op.type == OpType::Produce) {
      auto produce = MakeENEProduce(pool_size, mf_size);
      plan = produce;  // top level operator
      current = plan;
    } else {
      SPDLOG_ERROR("Unknown operator {}", op.type);
    }
  }
  return plan;
}

inline TCursorPtr MakePullOnce() { return std::make_unique<TCursorOnce>(nullptr); }
inline TCursorPtr MakePullScanAll(TCursorPtr &&input, int scan_all_elems) {
  auto data_fun = [scan_all_elems](TFrame &, TExecutionContext &) {
    std::vector<TFrame> frames(scan_all_elems);
    for (int i = 0; i < scan_all_elems; ++i) {
      frames.push_back(TFrame{});
    }
    return frames;
  };
  return std::make_unique<TCursorScanAll<decltype(data_fun)>>(std::move(input), std::move(data_fun));
}
inline TCursorPtr MakePullProduce(TCursorPtr &&input) { return std::make_unique<TCursorProduce>(std::move(input)); }
inline TCursorPtr MakePullPlan(const std::vector<Op> &ops) {
  std::vector<Op> reversed_ops(ops.rbegin(), ops.rend());
  TCursorPtr plan = nullptr;
  for (const auto &op : reversed_ops) {
    if (op.type == OpType::Once) {
      plan = MakePullOnce();
    } else if (op.type == OpType::ScanAll) {
      plan = MakePullScanAll(std::move(plan), op.props[SCANALL_ELEMS_POS]);
    } else if (op.type == OpType::Produce) {
      plan = MakePullProduce(std::move(plan));
    } else {
      SPDLOG_ERROR("Unknown operator {}", op.type);
    }
  }
  return plan;
}

// TODO(gitbuda): Here should be some https://en.wikipedia.org/wiki/Topological_sorting container.
inline std::shared_ptr<execution::DataOperator> MakeAsyncPlan(const std::vector<Op> &ops, int pool_size, int mf_size) {
  std::vector<Op> reversed_ops(ops.rbegin(), ops.rend());
  std::shared_ptr<execution::DataOperator> plan = nullptr;
  auto current = plan;
  // TODO(gitbuda): This looks messy, implement factory functions.
  int scan_all_name_cnt = 0;
  for (const auto &op : reversed_ops) {
    if (op.type == OpType::Once) {
      auto once_ptr = std::make_shared<execution::DataOperator>(execution::DataOperator{
          .name = "Once", .children = {}, .data_pool = std::make_unique<TDataPool>(pool_size, mf_size)});
      execution::Once once_state{.op = once_ptr.get()};
      once_ptr->execution.state = once_state;
      once_ptr->execution.status.has_more = true;
      current = once_ptr;

    } else if (op.type == OpType::ScanAll) {
      scan_all_name_cnt++;
      auto scan_all_ptr = std::make_shared<execution::DataOperator>(
          execution::DataOperator{.name = fmt::format("ScanAll_{}", scan_all_name_cnt),
                                  .children = {current},
                                  .data_pool = std::make_unique<TDataPool>(pool_size, mf_size)});
      execution::ScanAll scan_all_state{
          .op = scan_all_ptr.get(), .children = {current.get()}, .scan_all_elems = op.props[SCANALL_ELEMS_POS]};
      scan_all_ptr->execution.state = std::move(scan_all_state);
      scan_all_ptr->execution.status.has_more = true;
      current = scan_all_ptr;

    } else if (op.type == OpType::Produce) {
      auto produce_ptr = std::make_shared<execution::DataOperator>(execution::DataOperator{
          .name = "Produce", .children = {current}, .data_pool = std::make_unique<TDataPool>(pool_size, mf_size)});
      execution::Produce produce_state{.op = produce_ptr.get(), .children = {current.get()}};
      produce_ptr->execution.state = std::move(produce_state);
      produce_ptr->execution.status.has_more = true;
      current = produce_ptr;
      plan = produce_ptr;

    } else {
      SPDLOG_ERROR("Unknown operator {}", op.type);
    }
  }
  return plan;
  /* NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks) */
}

}  // namespace memgraph::query::v2::physical::mock
