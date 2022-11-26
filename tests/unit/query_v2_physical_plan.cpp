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

#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include "query/v2/physical/physical.hpp"
#include "utils/logging.hpp"
#include "utils/thread_pool.hpp"

namespace memgraph::query::v2::tests {

using Op = physical::mock::Op;
using OpType = physical::mock::OpType;
auto SCANALL_ELEMS_POS = physical::mock::SCANALL_ELEMS_POS;

using TDataPool = physical::multiframe::MPMCMultiframeFCFSPool;
using TPhysicalOperator = physical::PhysicalOperator<TDataPool>;
using TPhysicalOperatorPtr = std::shared_ptr<TPhysicalOperator>;
using TOnceOperator = physical::OncePhysicalOperator<TDataPool>;
template <typename TDataFun>
using TScanAllOperator = physical::ScanAllPhysicalOperator<TDataFun, TDataPool>;
using TProduceOperator = physical::ProducePhysicalOperator<TDataPool>;

class PhysicalPlanFixture : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
  utils::ThreadPool thread_pool_{16};
};

RC_GTEST_FIXTURE_PROP(PhysicalPlanFixture, PropertyBasedPhysicalPlan, ()) {
  SPDLOG_INFO("--- TEST START ----");
  int multiframes_no_per_op = *rc::gen::inRange(1, 32);
  int multiframe_size = *rc::gen::inRange(1, 2000);
  SPDLOG_INFO("#MF: {}, #F: {}", multiframes_no_per_op, multiframe_size);

  std::vector<rc::Gen<Op>> gens;
  gens.push_back(rc::gen::construct<Op>(rc::gen::element(OpType::ScanAll),
                                        rc::gen::container<std::vector<int>>(1, rc::gen::inRange(0, 100))));
  std::vector<Op> ops = {Op{.type = OpType::Produce}};
  const auto body =
      *rc::gen::container<std::vector<Op>>(*rc::gen::inRange(1, 4), rc::gen::join(rc::gen::elementOf(gens)));
  ops.insert(ops.end(), body.begin(), body.end());
  ops.push_back(Op{.type = OpType::Once});
  log_ops(ops);

  // TODO(gitbuda): Inject random sleeps in during Operator::Execute.

  TPhysicalOperatorPtr plan = nullptr;
  auto current = plan;
  for (const auto &op : ops) {
    if (op.type == OpType::Once) {
      auto data_pool = std::make_unique<TDataPool>(multiframes_no_per_op, multiframe_size);
      auto once = std::make_shared<TOnceOperator>(TOnceOperator("Physical Once", std::move(data_pool)));
      current->AddChild(once);
      current = once;

    } else if (op.type == OpType::ScanAll) {
      auto data_fun = [&op](TDataPool::TMultiframe &mf, physical::ExecutionContext &) {
        std::vector<physical::mock::Frame> frames;
        for (int i = 0; i < op.props[SCANALL_ELEMS_POS]; ++i) {
          for (int j = 0; j < mf.Data().size(); ++j) {
            frames.push_back(physical::mock::Frame{});
          }
        }
        return frames;
      };
      auto data_pool = std::make_unique<TDataPool>(multiframes_no_per_op, multiframe_size);
      auto scan_all = std::make_shared<TScanAllOperator<decltype(data_fun)>>(
          TScanAllOperator<decltype(data_fun)>("Physical ScanAll", std::move(data_fun), std::move(data_pool)));
      current->AddChild(scan_all);
      current = scan_all;

    } else if (op.type == OpType::Produce) {
      auto data_pool = std::make_unique<TDataPool>(multiframes_no_per_op, multiframe_size);
      plan = std::make_shared<TProduceOperator>(TProduceOperator("Physical Produce", std::move(data_pool)));
      current = plan;

    } else {
      SPDLOG_ERROR("Unknown operator {}", op.type);
    }
  }

  int64_t scan_all_cnt{1};
  for (const auto &op : ops) {
    if (op.type == OpType::ScanAll) {
      scan_all_cnt *= op.props[SCANALL_ELEMS_POS];
    }
  }
  SPDLOG_INFO("Total ScanAll elements: {}", scan_all_cnt);

  SPDLOG_INFO("-- EXECUTION START --");
  physical::ExecutionContext ctx{.thread_pool = &thread_pool_};
  plan->Execute(ctx);
  const auto &stats = plan->GetStats();
  ASSERT_EQ(stats.processed_frames, scan_all_cnt);
  SPDLOG_INFO("-- EXECUTION DONE --");
  SPDLOG_INFO("--- TEST END ----");
}

}  // namespace memgraph::query::v2::tests
