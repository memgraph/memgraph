#include "common.h"

#define THREADS_NO 8
constexpr size_t op_per_thread = 1e5;
// Depending on value there is a possiblity of numerical overflow
constexpr size_t max_number = 10;
constexpr size_t no_insert_for_one_delete = 2;

// This test checks remove method under pressure.
// Threads will try to insert and remove keys aproximetly in the same order.
// This will force threads to compete intensly with each other.
// Calls of remove method are interleaved with insert calls.
int main()
{
  memory_check(THREADS_NO, [] {
    skiplist_t skiplist;

    auto futures = run<std::pair<long long, long long>>(
        THREADS_NO, skiplist, [](auto acc, auto index) {
          auto rand_op = rand_gen_bool(no_insert_for_one_delete);
          size_t downcount = op_per_thread;
          long long sum = 0;
          long long count = 0;

          for (int i = 0; downcount > 0; i++) {
            auto data = i % max_number;
            if (rand_op()) {
              auto t = i;
              while (t > 0) {
                if (acc.remove(t)) {
                  sum -= t % max_number;
                  downcount--;
                  count--;
                  break;
                }
                t--;
              }
            } else {
              if (acc.insert(i, data).second) {
                sum += data;
                count++;
                downcount--;
              }
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
    permanent_assert(sums == 0, "Aproximetly Same values are present");
    check_size(accessor, counters);
  });
}
