#include "common.h"

constexpr size_t THREADS_NO = std::min(max_no_threads, 4);
constexpr size_t op_per_thread = 1e5;
constexpr size_t up_border_bit_set_pow2 = 3;
constexpr size_t key_range =
    op_per_thread * THREADS_NO * (1 << up_border_bit_set_pow2) * 2;

int main()
{
    DynamicBitset<> db;
    auto seted =
        collect_set(run<std::vector<bool>>(THREADS_NO, [&](auto index) {
            auto rand = rand_gen(key_range);
            auto rand_len = rand_gen(up_border_bit_set_pow2);
            std::vector<bool> set(key_range + (1 << up_border_bit_set_pow2));

            for (size_t i = 0; i < op_per_thread; i++) {
                auto len = 1 << rand_len();
                size_t num = (rand() / len) * len;
                db.set(num, len);
                for (int j = 0; j < len; j++)
                    set[num + j] = true;
            }

            return set;
        }));

    auto cleared =
        collect_set(run<std::vector<bool>>(THREADS_NO, [&](auto index) {
            auto rand = rand_gen(key_range);
            auto rand_len = rand_gen(up_border_bit_set_pow2);
            std::vector<bool> set(key_range + (1 << up_border_bit_set_pow2));

            for (size_t i = 0; i < op_per_thread; i++) {
                auto len = 1 << rand_len();
                size_t num = (rand() / len) * len;
                for (int j = 0; j < len; j++) {
                    set[num + j] = set[num + j] | db.at(num + j);
                }
                db.clear(num, len);
            }

            return set;
        }));

    for (size_t i = 0; i < seted.size(); i++) {
        seted[i] = seted[i] & (!cleared[i]);
    }

    check_set(db, seted);
}
