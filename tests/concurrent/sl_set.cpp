#include "common.hpp"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t key_range = 1e4;
constexpr size_t op_per_thread = 1e5;
constexpr size_t no_insert_for_one_delete = 2;

// TODO: document the test

// This test checks set.
// Each thread removes random data. So removes are joint.
// Calls of remove method are interleaved with insert calls.
int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  memory_check(THREADS_NO, [] {
    ConcurrentSet<std::string> skiplist;

    auto futures =
        run<std::vector<long>>(THREADS_NO, skiplist, [](auto acc, auto index) {
          auto rand = rand_gen(key_range);
          auto rand_op = rand_gen_bool(no_insert_for_one_delete);
          long long downcount = op_per_thread;
          std::vector<long> set(key_range);

          do {
            int num = rand();
            std::string num_str = std::to_string(num);
            if (rand_op()) {
              if (acc.remove(num_str)) {
                downcount--;
                set[num]--;
              }
            } else {
              std::string num_str = std::to_string(num);
              if (acc.insert(num_str).second) {
                downcount--;
                set[num]++;
              }
            }
          } while (downcount > 0);

          return set;
        });

    long set[key_range] = {0};
    for (auto &data : collect(futures)) {
      for (int i = 0; i < key_range; i++) {
        set[i] += data.second[i];
      }
    }

    auto accessor = skiplist.access();
    for (int i = 0; i < key_range; i++) {
      permanent_assert(set[i] == 0 || set[i] == 1 ||
                           (set[i] == 1) ^ accessor.contains(std::to_string(i)),
                       "Set doesn't hold it's guarantees.");
    }

    for (auto &e : accessor) {
      set[std::stoi(e)]--;
    }

    check_zero(key_range, set, "Set");
  });
}
