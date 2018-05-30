#include <chrono>
#include <memory>
#include <unordered_set>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/future.hpp"
#include "utils/thread.hpp"
#include "utils/timer.hpp"

TEST(ThreadPool, RunMany) {
  utils::ThreadPool tp(10);
  const int kResults = 10000;
  std::vector<utils::Future<int>> results;
  for (int i = 0; i < kResults; ++i) {
    results.emplace_back(tp.Run([i]() { return i; }));
  }

  std::unordered_set<int> result_set;
  for (auto &result : results) result_set.insert(result.get());
  EXPECT_EQ(result_set.size(), kResults);
}

TEST(ThreadPool, EnsureParallel) {
  using namespace std::chrono_literals;

  const int kSize = 10;
  utils::ThreadPool tp(kSize);
  std::vector<utils::Future<void>> results;

  utils::Timer t;
  for (int i = 0; i < kSize; ++i) {
    results.emplace_back(tp.Run([]() { std::this_thread::sleep_for(50ms); }));
  }
  for (auto &res : results) res.wait();

  auto elapsed = t.Elapsed();
  EXPECT_GE(elapsed, 30ms);
  EXPECT_LE(elapsed, 200ms);
}
