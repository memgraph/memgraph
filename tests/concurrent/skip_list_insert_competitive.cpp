#include <atomic>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "utils/skip_list.hpp"

const int kNumThreads = 8;
const uint64_t kMaxNum = 10000000;

int main() {
  utils::SkipList<uint64_t> list;

  std::atomic<uint64_t> success{0};

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.push_back(std::thread([&list, &success] {
      for (uint64_t num = 0; num < kMaxNum; ++num) {
        auto acc = list.access();
        if (acc.insert(num).second) {
          success.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }));
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }
  CHECK(success == kMaxNum);

  CHECK(list.size() == kMaxNum);
  for (uint64_t i = 0; i < kMaxNum; ++i) {
    auto acc = list.access();
    auto it = acc.find(i);
    CHECK(it != acc.end());
    CHECK(*it == i);
  }

  return 0;
}
