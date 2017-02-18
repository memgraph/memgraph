#include "common.h"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t op_per_thread = 1e5;
constexpr size_t key_range = op_per_thread * THREADS_NO * 3;

// TODO: document the test

int main() {
  DynamicBitset<> db;

  auto set = collect_set(run<std::vector<bool>>(THREADS_NO, [&](auto index) {
    auto rand = rand_gen(key_range);
    std::vector<bool> set(key_range);

    for (size_t i = 0; i < op_per_thread; i++) {
      size_t num = rand();
      db.set(num);
      set[num] = true;
    }

    return set;
  }));

  check_set(db, set);
}
