#include "common.hpp"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t op_per_thread = 1e5;
constexpr size_t bit_part_len = 2;
constexpr size_t no_slots = 1e4;
constexpr size_t key_range = no_slots * THREADS_NO * bit_part_len;
constexpr size_t no_sets_per_clear = 2;

// TODO: document the test

int main() {
  DynamicBitset<> db;

  auto seted = collect_set(run<std::vector<bool>>(THREADS_NO, [&](auto index) {
    auto rand = rand_gen(no_slots);
    auto clear_op = rand_gen_bool(no_sets_per_clear);
    std::vector<bool> set(key_range);

    for (size_t i = 0; i < op_per_thread; i++) {
      size_t num = rand() * THREADS_NO * bit_part_len + index * bit_part_len;

      if (clear_op()) {
        db.clear(num, bit_part_len);
        for (int j = 0; j < bit_part_len; j++) {
          set[num + j] = false;
        }
      } else {
        db.set(num, bit_part_len);
        for (int j = 0; j < bit_part_len; j++) set[num + j] = true;
      }
    }

    return set;
  }));

  check_set(db, seted);
}
