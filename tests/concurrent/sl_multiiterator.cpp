#include "common.h"

#define THREADS_NO 8
constexpr size_t key_range = 1e4;
constexpr size_t op_per_thread = 1e5;
// Depending on value there is a possiblity of numerical overflow
constexpr size_t max_number = 10;
constexpr size_t no_insert_for_one_delete = 1;

// This test checks MultiIterator from multimap.
// Each thread removes random data. So removes are joint.
// Calls of remove method are interleaved with insert calls which always
// succeed.
int main()
{
    init_log();
    memory_check(THREADS_NO, [] {
        multimap_t skiplist;

        auto futures = run<std::vector<long long>>(
            THREADS_NO, skiplist, [](auto acc, auto index) {
                auto rand = rand_gen(key_range);
                auto rand_op = rand_gen_bool(no_insert_for_one_delete);
                long long downcount = op_per_thread;
                std::vector<long long> set(key_range, 0);

                do {
                    size_t num = rand();
                    auto data = num % max_number;
                    if (rand_op()) {
                        if (acc.remove(num)) {
                            downcount--;
                            set[num]--;
                        }
                    } else {
                        acc.insert(num, data);
                        downcount--;
                        set[num]++;
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
        check_multi_iterator(accessor, key_range, set);
        check_order<multimap_t>(accessor);
    });
}
