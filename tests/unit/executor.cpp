#include <atomic>
#include <memory>

#include "gtest/gtest.h"

#include "utils/executor.hpp"

TEST(Executor, DontRun) {
  std::atomic<int> count{0};
  {
    Executor exec(std::chrono::seconds(0));
    // Be sure executor is sleeping.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    exec.RegisterJob([&count]() { ++count; });
    // Try to wait to test if executor might somehow become awake and execute
    // the job.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  EXPECT_EQ(count, 0);
}

TEST(Executor, Run) {
  std::atomic<int> count{0};
  {
    Executor exec(std::chrono::milliseconds(500));
    // Be sure executor is sleeping.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    exec.RegisterJob([&count]() { ++count; });
    exec.RegisterJob([&count]() { ++count; });
    exec.RegisterJob([&count]() { ++count; });

    // Be sure executor execute thread is triggered
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  EXPECT_EQ(count, 3);
}

TEST(Executor, RunUnregister) {
  std::atomic<int> count1{0};
  std::atomic<int> count2{0};
  {
    Executor exec(std::chrono::milliseconds(500));
    // Be sure executor is sleeping.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto job = exec.RegisterJob([&count1]() { ++count1; });
    exec.RegisterJob([&count2]() { ++count2; });

    // Be sure executor execute thread is triggered
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    exec.UnRegisterJob(job);

    // Be sure executor execute thread is triggered
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  EXPECT_EQ(count1, 1);
  EXPECT_EQ(count2, 2);
}
