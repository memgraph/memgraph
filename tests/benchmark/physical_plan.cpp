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

#include <thread>

#include <benchmark/benchmark.h>

#include "query/v2/physical/mock/mock.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "query/v2/physical/physical_ene.hpp"
#include "query/v2/physical/physical_pull.hpp"

static const std::size_t kThreadsNum = std::thread::hardware_concurrency();

using namespace memgraph::query::v2::physical;
using namespace memgraph::query::v2::physical::mock;

using TDataPool = multiframe::MPMCMultiframeFCFSPool;
using TPhysicalOperator = PhysicalOperator<TDataPool>;
using TPhysicalOperatorPtr = std::shared_ptr<TPhysicalOperator>;
using TOnceOperator = OncePhysicalOperator<TDataPool>;
template <typename TDataFun>
using TScanAllOperator = ScanAllPhysicalOperator<TDataFun, TDataPool>;
using TProduceOperator = ProducePhysicalOperator<TDataPool>;
using TExecutionContext = ExecutionContext;

class PhysicalFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &) { thread_pool_ = std::make_unique<memgraph::utils::ThreadPool>(kThreadsNum); }
  void TearDown(const benchmark::State &) {}

  std::unique_ptr<memgraph::utils::ThreadPool> thread_pool_;
};

BENCHMARK_DEFINE_F(PhysicalFixture, TestENESingleThread)
(benchmark::State &state) {
  int pool_size = state.range(0);
  int mf_size = state.range(1);
  int scan_all_elems = 1000;
  std::vector<Op> ops{
      Op{.type = OpType::Produce},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::Once},
  };

  for (auto _ : state) {
    auto plan = MakeENEPlan(ops, pool_size, mf_size);
    TExecutionContext ctx{.thread_pool = thread_pool_.get()};
    plan->Execute(ctx);
  }
}
BENCHMARK_REGISTER_F(PhysicalFixture, TestENESingleThread)
    ->ArgsProduct({
        benchmark::CreateRange(4, 16, 2),
        benchmark::CreateRange(10, 10000, 10),
    })
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_DEFINE_F(PhysicalFixture, TestCursorPull)
(benchmark::State &state) {
  int scan_all_elems = 1000;
  std::vector<Op> ops{
      Op{.type = OpType::Produce},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::ScanAll, .props = {scan_all_elems}},
      Op{.type = OpType::Once},
  };

  for (auto _ : state) {
    auto plan = memgraph::query::v2::physical::mock::MakePullPlan(ops);
    memgraph::query::v2::physical::mock::Frame frame;
    TExecutionContext ctx{.thread_pool = thread_pool_.get()};
    while (plan->Pull(frame, ctx))
      ;
  }
}
BENCHMARK_REGISTER_F(PhysicalFixture, TestCursorPull)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_MAIN();
