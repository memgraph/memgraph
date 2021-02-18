#include "skip_list_common.hpp"

#include "utils/skip_list.hpp"

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  utils::SkipList<int64_t> list;

  RunConcurrentTest([&list](auto *run, auto *stats) {
    std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(0, 3);
    while (run->load(std::memory_order_relaxed)) {
      int value = distribution(generator);
      auto accessor = list.access();
      switch (value) {
        case 0:
          stats->succ[OP_INSERT] += static_cast<uint64_t>(accessor.insert(5).second);
          break;
        case 1:
          stats->succ[OP_CONTAINS] += static_cast<uint64_t>(accessor.contains(5));
          break;
        case 2:
          stats->succ[OP_REMOVE] += static_cast<uint64_t>(accessor.remove(5));
          break;
        case 3:
          stats->succ[OP_FIND] += static_cast<uint64_t>(accessor.find(5) != accessor.end());
          break;
        default:
          std::terminate();
      }
      ++stats->total;
    }
  });

  return 0;
}
