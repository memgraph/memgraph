#include "common.hpp"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);

constexpr size_t elems_per_thread = 100000;
constexpr size_t key_range = elems_per_thread * THREADS_NO * 2;

// TODO: document the test

// This test checks insert_unique method under pressure.
// Test checks for missing data and changed/overwriten data.
int main(int, char **argv) {
  google::InitGoogleLogging(argv[0]);
  map_t skiplist;

  auto futures =
      run<std::vector<size_t>>(THREADS_NO, skiplist, [](auto acc, auto index) {
        auto rand = rand_gen(key_range);
        long long downcount = elems_per_thread;
        std::vector<size_t> owned;

        do {
          auto key = rand();
          if (acc.insert(key, index).second) {
            downcount--;
            owned.push_back(key);
          }
        } while (downcount > 0);

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
