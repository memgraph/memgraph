#include "common.h"

#define THREADS_NO 8
constexpr size_t key_range = 1e5;
constexpr size_t op_per_thread = 1e6;
// Depending on value there is a possiblity of numerical overflow
constexpr size_t max_number = 10;
constexpr size_t no_find_per_change = 5;
constexpr size_t no_insert_for_one_delete = 1;

// This test simulates behavior of transactions.
// Each thread makes a series of finds interleaved with method which change.
// Exact ratio of finds per change and insert per delete can be regulated with
// no_find_per_change and no_insert_for_one_delete.
int main()
{
  memory_check(THREADS_NO, [] {
    skiplist_t skiplist;

    auto futures = run<std::pair<long long, long long>>(
        THREADS_NO, skiplist, [](auto acc, auto index) {
          auto rand = rand_gen(key_range);
          auto rand_change = rand_gen_bool(no_find_per_change);
          auto rand_delete = rand_gen_bool(no_insert_for_one_delete);
          long long sum = 0;
          long long count = 0;

          for (int i = 0; i < op_per_thread; i++) {
            auto num = rand();
            auto data = num % max_number;
            if (rand_change()) {
              if (rand_delete()) {
                if (acc.remove(num)) {
                  sum -= data;
                  count--;
                }
              } else {
                if (acc.insert(num, data).second) {
                  sum += data;
                  count++;
                }
              }
            } else {
              auto value = acc.find(num);
              permanent_assert(value == acc.end() || value->second == data,
                               "Data is invalid");
            }
          }

          return std::pair<long long, long long>(sum, count);
        });

    auto accessor = skiplist.access();
    long long sums = 0;
    long long counters = 0;
    for (auto &data : collect(futures)) {
      sums += data.second.first;
      counters += data.second.second;
    }

    for (auto &e : accessor) {
      sums -= e.second;
    }
    permanent_assert(sums == 0, "Same values aren't present");
    check_size(accessor, counters);
  });
}
