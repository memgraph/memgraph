#include "common.h"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t key_range = 1e2;
constexpr size_t op_per_thread = 1e4;
// Depending on value there is a possiblity of numerical overflow
constexpr size_t max_number = 10;
constexpr size_t no_find_per_change = 2;
constexpr size_t no_insert_for_one_delete = 1;

// This test simulates behavior of a transactions.
// Each thread makes a series of finds interleaved with method which change.
// Exact ratio of finds per change and insert per delete can be regulated with
// no_find_per_change and no_insert_for_one_delete.
int main() {
  init_log();
  memory_check(THREADS_NO, [] {
    ConcurrentList<std::pair<int, int>> list;
    permanent_assert(list.size() == 0, "The list isn't empty");

    auto futures = run<std::pair<long long, long long>>(
        THREADS_NO, [&](auto index) mutable {
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
                for (auto it = list.begin(); it != list.end(); it++) {
                  if (it->first == num) {
                    if (it.remove()) {
                      sum -= data;
                      count--;
                    }
                    break;
                  }
                }
              } else {
                list.begin().push(std::make_pair(num, data));
                sum += data;
                count++;
              }
            } else {
              for (auto &v : list) {
                if (v.first == num) {
                  permanent_assert(v.second == data, "Data is invalid");
                  break;
                }
              }
            }
          }

          return std::pair<long long, long long>(sum, count);
        });

    auto it = list.begin();
    long long sums = 0;
    long long counters = 0;
    for (auto &data : collect(futures)) {
      sums += data.second.first;
      counters += data.second.second;
    }

    for (auto &e : list) {
      sums -= e.second;
    }

    permanent_assert(sums == 0, "Same values aren't present");
    check_size_list<ConcurrentList<std::pair<int, int>>>(list, counters);

    std::this_thread::sleep_for(1s);
  });
}
