#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "utils/stack.hpp"
#include "utils/timer.hpp"

const int kNumThreads = 4;

DEFINE_int32(max_value, 100000000, "Maximum value that should be inserted");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  utils::Stack<uint64_t, 8190> stack;

  std::vector<std::thread> threads;
  utils::Timer timer;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.push_back(std::thread([&stack, i] {
      for (uint64_t item = i; item < FLAGS_max_value; item += kNumThreads) {
        stack.Push(item);
      }
    }));
  }

  std::atomic<bool> run{true};
  std::thread verify([&stack, &run] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::vector<bool> found;
    found.resize(FLAGS_max_value);
    std::optional<uint64_t> item;
    while (run || (item = stack.Pop())) {
      if (item) {
        CHECK(*item < FLAGS_max_value);
        found[*item] = true;
      }
    }
    CHECK(!stack.Pop());
    for (uint64_t i = 0; i < FLAGS_max_value; ++i) {
      CHECK(found[i]);
    }
  });

  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }

  auto elapsed = timer.Elapsed().count();

  run.store(false);
  verify.join();

  std::cout << "Duration: " << elapsed << std::endl;
  std::cout << "Throughput: " << FLAGS_max_value / elapsed << std::endl;

  return 0;
}
