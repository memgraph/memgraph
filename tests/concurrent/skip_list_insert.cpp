#include <thread>
#include <vector>

#include <glog/logging.h>

#include "utils/skip_list.hpp"

const int kNumThreads = 8;
const uint64_t kMaxNum = 10000000;

int main() {
  utils::SkipList<uint64_t> list;

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.push_back(std::thread([&list, i] {
      for (uint64_t num = i * kMaxNum; num < (i + 1) * kMaxNum; ++num) {
        auto acc = list.access();
        CHECK(acc.insert(num).second);
      }
    }));
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }

  CHECK(list.size() == kMaxNum * kNumThreads);
  for (uint64_t i = 0; i < kMaxNum * kNumThreads; ++i) {
    auto acc = list.access();
    auto it = acc.find(i);
    CHECK(it != acc.end());
    CHECK(*it == i);
  }

  return 0;
}
