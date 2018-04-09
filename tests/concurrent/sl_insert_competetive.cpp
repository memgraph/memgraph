#include "common.hpp"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t elems_per_thread = 100000;

// TODO: document the test

// This test checks insert_unique method under pressure.
// Threads will try to insert keys in the same order.
// This will force threads to compete intensly with each other.
// Test checks for missing data and changed/overwriten data.
int main(int, char **argv) {
  google::InitGoogleLogging(argv[0]);
  map_t skiplist;

  auto futures =
      run<std::vector<size_t>>(THREADS_NO, skiplist, [](auto acc, auto index) {
        long long downcount = elems_per_thread;
        std::vector<size_t> owned;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfor-loop-analysis"
        for (int i = 0; downcount > 0; i++) {
          if (acc.insert(i, index).second) {
            downcount--;
            owned.push_back(i);
          }
        }
#pragma GCC diagnostic pop
        check_present_same(acc, index, owned);
        return owned;
      });

  auto accessor = skiplist.access();
  for (auto &owned : collect(futures)) {
    check_present_same(accessor, owned);
  }

  check_size(accessor, THREADS_NO * elems_per_thread);
  check_order(accessor);
}
