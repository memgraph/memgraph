#include "common.h"

#define THREADS_NO 8
constexpr size_t key_range = 1e4;
constexpr size_t op_per_thread = 1e5;
constexpr size_t no_insert_for_one_delete = 1;

// This test checks multiset.
// Each thread removes random data. So removes are joint.
// Calls of remove method are interleaved with insert calls which always
// succeed.
int main()
{
    memory_check(THREADS_NO, [] {
        multiset_t skiplist;

        auto futures = run<std::vector<long>>(
            THREADS_NO, skiplist, [](auto acc, auto index) {
                auto rand = rand_gen(key_range);
                auto rand_op = rand_gen_bool(no_insert_for_one_delete);
                long long downcount = op_per_thread;
                std::vector<long> set(key_range, 0);

                do {
                    size_t num = rand();
                    if (rand_op()) {
                        if (acc.remove(num)) {
                            downcount--;
                            set[num]--;
                        }
                    } else {
                        acc.insert(num);
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
        for (int i = 0; i < key_range; i++) {
            auto it = accessor.find(i);
            if (set[i] > 0) {
                for (int j = 0; j < set[i]; j++) {
                    permanent_assert(
                        it == i,
                        "Iterator doesn't iterate through same key entrys");
                    it++;
                }
            }
            permanent_assert(it == accessor.end() || it != i,
                             "There is more data than it should be.");
        }

        for (auto &e : accessor) {
            set[e]--;
        }

        check_zero(key_range, set, "MultiSet");

    });
}
