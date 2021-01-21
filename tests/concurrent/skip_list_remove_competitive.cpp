#include <atomic>
#include <thread>
#include <vector>

#include "utils/skip_list.hpp"

const int kNumThreads = 8;
const uint64_t kMaxNum = 10000000;

int main() {
  utils::SkipList<uint64_t> list;

  for (int i = 0; i < kMaxNum; ++i) {
    auto acc = list.access();
    auto ret = acc.insert(i);
    MG_ASSERT(ret.first != acc.end());
    MG_ASSERT(ret.second);
  }

  std::atomic<uint64_t> success{0};

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.push_back(std::thread([&list, &success] {
      for (uint64_t num = 0; num < kMaxNum; ++num) {
        auto acc = list.access();
        if (acc.remove(num)) {
          success.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }));
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }
  MG_ASSERT(success == kMaxNum);

  MG_ASSERT(list.size() == 0);
  uint64_t count = 0;
  auto acc = list.access();
  for (auto it = acc.begin(); it != acc.end(); ++it) {
    ++count;
  }
  MG_ASSERT(count == 0);

  return 0;
}
