#include <thread>
#include <vector>

#include "utils/skip_list.hpp"

const int kNumThreads = 8;
const uint64_t kMaxNum = 10000000;

int main() {
  utils::SkipList<uint64_t> list;

  for (int i = 0; i < kMaxNum * kNumThreads; ++i) {
    auto acc = list.access();
    auto ret = acc.insert(i);
    MG_ASSERT(ret.first != acc.end());
    MG_ASSERT(ret.second);
  }

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.push_back(std::thread([&list, i] {
      for (uint64_t num = i * kMaxNum; num < (i + 1) * kMaxNum; ++num) {
        auto acc = list.access();
        MG_ASSERT(acc.remove(num));
      }
    }));
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }

  MG_ASSERT(list.size() == 0);
  uint64_t count = 0;
  auto acc = list.access();
  for (auto it = acc.begin(); it != acc.end(); ++it) {
    ++count;
  }
  MG_ASSERT(count == 0);

  return 0;
}
