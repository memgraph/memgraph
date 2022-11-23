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
#include "utils/thread_pool.hpp"
#include "utils/timer.hpp"

namespace memgraph::query::v2::tests {

class MultiframePoolFixture : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    thread_pool_.Shutdown();
  }

  physical::multiframe::MPMCMultiframeFCFSPool multiframe_pool_{16, 100};
  utils::ThreadPool thread_pool_{16};
};

class PhysicalPlanFixture : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(MultiframePoolFixture, DISABLED_ConcurrentMultiframePoolAccess) {
  std::atomic<int> readers_got_access_cnt;
  std::atomic<int> writers_got_access_cnt;
  utils::Timer timer;

  for (int i = 0; i < 1000000; ++i) {
    // Add readers
    thread_pool_.AddTask([&]() {
      while (true) {
        auto token = multiframe_pool_.GetFull();
        if (token) {
          ASSERT_TRUE(token->id >= 0 && token->id < 16);
          readers_got_access_cnt.fetch_add(1);
          multiframe_pool_.ReturnEmpty(token->id);
          break;
        }
      }
    });
    // Add writers
    thread_pool_.AddTask([&]() {
      while (true) {
        auto token = multiframe_pool_.GetEmpty();
        if (token) {
          ASSERT_TRUE(token->id >= 0 && token->id < 16);
          writers_got_access_cnt.fetch_add(1);
          multiframe_pool_.ReturnFull(token->id);
          break;
        }
      }
    });
  }
  std::cout << "All readers and writters scheduled" << std::endl;

  while (thread_pool_.UnfinishedTasksNum() != 0) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  std::cout << "TIME: " << timer.Elapsed<std::chrono::milliseconds>().count() << std::endl;

  ASSERT_EQ(readers_got_access_cnt.load(), 1000000);
  ASSERT_EQ(writers_got_access_cnt.load(), 1000000);
}

enum class OpType { Once, ScanAll, Produce };
std::ostream &operator<<(std::ostream &os, const OpType &op_type) {
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
constexpr int ENTITIES_NUM = 0;
struct Op {
  OpType type;
  std::vector<int> props;
};

RC_GTEST_FIXTURE_PROP(PhysicalPlanFixture, PropertyBasedPhysicalPlan, ()) {
  using TDataPool = physical::multiframe::MPMCMultiframeFCFSPool;
  using TPhysicalOperator = physical::PhysicalOperator<TDataPool>;
  using TPhysicalOperatorPtr = std::shared_ptr<TPhysicalOperator>;
  using TOnceOperator = physical::OncePhysicalOperator<TDataPool>;
  using TProduceOperator = physical::ProducePhysicalOperator<TDataPool>;

  int multiframes_no_per_op = *rc::gen::inRange(1, 32);
  int multiframe_size = *rc::gen::inRange(1, 2000);
  std::cout << "#MF: " << multiframes_no_per_op << " #F: " << multiframe_size << std::endl;

  std::vector<rc::Gen<Op>> gens;
  gens.push_back(rc::gen::construct<Op>(rc::gen::element(OpType::ScanAll),
                                        rc::gen::container<std::vector<int>>(1, rc::gen::inRange(1, 10000))));
  std::vector<Op> ops = {Op{.type = OpType::Produce}};
  const auto body =
      *rc::gen::container<std::vector<Op>>(*rc::gen::inRange(1, 4), rc::gen::join(rc::gen::elementOf(gens)));
  ops.insert(ops.end(), body.begin(), body.end());
  ops.push_back(Op{.type = OpType::Once});

  TPhysicalOperatorPtr plan = nullptr;
  auto current = plan;
  for (const auto &op : ops) {
    if (op.type == OpType::Once) {
      std::cout << op.type << std::endl;
      auto data_pool = std::make_unique<TDataPool>(multiframes_no_per_op, multiframe_size);
      auto once = std::make_shared<TOnceOperator>(TOnceOperator("Physical Once", std::move(data_pool)));
      current->AddChild(once);
      current = once;

    } else if (op.type == OpType::ScanAll) {
      std::cout << op.type << " elems: " << op.props[0] << std::endl;
      auto data_fun = [&op](physical::multiframe::Multiframe &, physical::ExecutionContext &) {
        std::vector<physical::Frame> frames;
        for (int i = 0; i < op.props[ENTITIES_NUM]; ++i) {
          frames.push_back(physical::Frame{});
        }
        return frames;
      };
      auto data_pool = std::make_unique<TDataPool>(multiframes_no_per_op, multiframe_size);
      auto scan_all = std::make_shared<physical::ScanAllPhysicalOperator<decltype(data_fun), TDataPool>>(
          physical::ScanAllPhysicalOperator<decltype(data_fun), TDataPool>("Physical ScanAll", std::move(data_fun),
                                                                           std::move(data_pool)));
      current->AddChild(scan_all);
      current = scan_all;

    } else if (op.type == OpType::Produce) {
      std::cout << op.type << std::endl;
      auto data_pool = std::make_unique<TDataPool>(multiframes_no_per_op, multiframe_size);
      plan = std::make_shared<TProduceOperator>(TProduceOperator("Physical Produce", std::move(data_pool)));
      current = plan;
    } else {
      std::cout << op.type << std::endl;
    }
  }

  physical::ExecutionContext ctx;
  plan->Execute(ctx);
}

}  // namespace memgraph::query::v2::tests
