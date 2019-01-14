#include <thread>
#include <vector>

#include <glog/logging.h>

#include "utils/skip_list.hpp"

const int kNumThreads = 8;
const uint64_t kMaxNum = 10000000;

int main() {
  utils::SkipList<uint64_t> list;

  for (int i = 0; i < kMaxNum * kNumThreads; ++i) {
    auto acc = list.access();
    auto ret = acc.insert(i);
    CHECK(ret.first != acc.end());
    CHECK(ret.second);
  }

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.push_back(std::thread([&list, i] {
      for (uint64_t num = i * kMaxNum; num < (i + 1) * kMaxNum; ++num) {
        auto acc = list.access();
        CHECK(acc.remove(num));
      }
    }));
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }

  CHECK(list.size() == 0);
  uint64_t count = 0;
  auto acc = list.access();
  for (auto it = acc.begin(); it != acc.end(); ++it) {
    ++count;
  }
  CHECK(count == 0);

  return 0;
}
