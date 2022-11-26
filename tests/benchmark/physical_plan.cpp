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

#include "query/v2/physical/mock.hpp"
#include "query/v2/physical/multiframe.hpp"
#include "query/v2/physical/physical.hpp"

static const std::size_t kThreadsNum = std::thread::hardware_concurrency();

using TFrame = memgraph::query::v2::physical::mock::Frame;
using TDataPool = memgraph::query::v2::physical::multiframe::MPMCMultiframeFCFSPool;
using TPhysicalOperator = memgraph::query::v2::physical::PhysicalOperator<TDataPool>;
using TPhysicalOperatorPtr = std::shared_ptr<TPhysicalOperator>;
using TOnceOperator = memgraph::query::v2::physical::OncePhysicalOperator<TDataPool>;
template <typename TDataFun>
using TScanAllOperator = memgraph::query::v2::physical::ScanAllPhysicalOperator<TDataFun, TDataPool>;
using TProduceOperator = memgraph::query::v2::physical::ProducePhysicalOperator<TDataPool>;
using TExecutionContext = memgraph::query::v2::physical::ExecutionContext;

class PhysicalFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &) { thread_pool_ = std::make_unique<memgraph::utils::ThreadPool>(kThreadsNum); }
  void TearDown(const benchmark::State &) {}

  std::unique_ptr<memgraph::utils::ThreadPool> thread_pool_;
  TPhysicalOperatorPtr plan_;
  TPhysicalOperatorPtr current_;
};

TPhysicalOperatorPtr MakeOnce(int pool_size, int mf_size) {
  auto data_pool = std::make_unique<TDataPool>(pool_size, mf_size);
  return std::make_shared<TOnceOperator>(TOnceOperator("Physical Once", std::move(data_pool)));
}

TPhysicalOperatorPtr MakeScanAll(int pool_size, int mf_size, int scan_all_elems) {
  auto data_fun = [&](TDataPool::TMultiframe &mf, TExecutionContext &) {
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

TPhysicalOperatorPtr MakeProduce(int pool_size, int mf_size) {
  auto data_pool = std::make_unique<TDataPool>(pool_size, mf_size);
  return std::make_shared<TProduceOperator>(TProduceOperator("Physical Produce", std::move(data_pool)));
}

BENCHMARK_DEFINE_F(PhysicalFixture, TestSingleThread)
(benchmark::State &state) {
  int pool_size = state.range(0);
  int mf_size = state.range(1);
  int scan_all_elems = 1000;
  int scan_all_no = 1;

  plan_ = MakeProduce(pool_size, mf_size);
  current_ = plan_;
  for (int i = 0; i < scan_all_no; ++i) {
    auto scan_all = MakeScanAll(pool_size, mf_size, scan_all_elems);
    current_->AddChild(scan_all);
    current_ = scan_all;
  }
  auto once = MakeOnce(pool_size, mf_size);
  current_->AddChild(once);

  for (auto _ : state) {
    TExecutionContext ctx{.thread_pool = thread_pool_.get()};
    plan_->Execute(ctx);
  }
}
// multiframes, frames, threads
BENCHMARK_REGISTER_F(PhysicalFixture, TestSingleThread)
    ->Args({2, 1})
    ->Args({2, 10})
    ->Args({2, 100})
    ->Args({2, 1000})
    // ->Args({1, 1})
    // ->Args({1, 10})
    // ->Args({1, 100})
    // TODO(gitbuda): Doesn't work...
    // ->Args({1, 1000})
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

BENCHMARK_MAIN();
