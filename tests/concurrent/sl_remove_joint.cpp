#include "common.h"

#define THREADS_NO 8
constexpr size_t key_range = 1e5;
constexpr size_t op_per_thread = 1e5;
// Depending on value there is a possiblity of numerical overflow
constexpr size_t max_number = 10;
constexpr size_t no_insert_for_one_delete = 2;

// This test checks remove method under pressure.
// Each thread removes random data. So removes are joint.
// Calls of remove method are interleaved with insert calls.
int main()
{
  memory_check(THREADS_NO, [] {
    skiplist_t skiplist;

    auto futures = run<std::pair<long long, long long>>(
        THREADS_NO, skiplist, [](auto acc, auto index) {
          auto rand = rand_gen(key_range);
          auto rand_op = rand_gen_bool(no_insert_for_one_delete);
          size_t downcount = op_per_thread;
          long long sum = 0;
          long long count = 0;

          do {
            auto num = rand();
            auto data = num % max_number;
            if (rand_op()) {
              if (acc.remove(num)) {
                sum -= data;
                downcount--;
                count--;
              }
            } else {
              if (acc.insert(num, data).second) {
                sum += data;
                downcount--;
                count++;
              }
            }
          } while (downcount > 0);

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
    permanent_assert(sums == 0, "Aproximetly Same values are present");
    check_size(accessor, counters);
  });
}
