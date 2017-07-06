#include "common.hpp"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t key_range = 1e5;
constexpr size_t op_per_thread = 1e6;
constexpr size_t no_insert_for_one_delete = 1;

// TODO: document the test

// This test checks remove method under pressure.
// Each thread removes it's own data. So removes are disjoint.
// Calls of remove method are interleaved with insert calls.
int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  memory_check(THREADS_NO, [] {
    map_t skiplist;

    auto futures = run<std::vector<size_t>>(
        THREADS_NO, skiplist, [](auto acc, auto index) {
          auto rand = rand_gen(key_range);
          auto rand_op = rand_gen_bool(no_insert_for_one_delete);
          long long downcount = op_per_thread;
          std::vector<size_t> owned;
          auto inserter =
              insert_try<size_t, size_t, map_t>(acc, downcount, owned);

          do {
            if (owned.size() != 0 && rand_op()) {
              auto rem = rand() % owned.size();
              permanent_assert(acc.remove(owned[rem]), "Owned data removed");
              owned.erase(owned.begin() + rem);
              downcount--;
            } else {
              inserter(rand(), index);
            }
          } while (downcount > 0);

          check_present_same<map_t>(acc, index, owned);
          return owned;
        });

    auto accessor = skiplist.access();
    size_t count = 0;
    for (auto &owned : collect(futures)) {
      check_present_same<map_t>(accessor, owned);
      count += owned.second.size();
    }
    check_size<map_t>(accessor, count);
    check_order<map_t>(accessor);
  });
}
