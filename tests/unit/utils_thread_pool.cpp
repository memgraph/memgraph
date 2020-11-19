#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include <utils/thread_pool.hpp>

using namespace std::chrono_literals;

TEST(ThreadPool, Basic) {
  constexpr size_t adder_count =
      500'000; constexpr std::array<size_t, 5> pool_sizes{1, 2, 4, 8, 100};

  for (const auto pool_size : pool_sizes) {
    utils::ThreadPool pool{pool_size};

    std::atomic<int> count{0};
    for (size_t i = 0; i < adder_count; ++i) {
      pool.AddTask([&] { count.fetch_add(1); });
    }

    while (pool.UnfinishedTasksNum() != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_EQ(count.load(), adder_count);
  }
}
