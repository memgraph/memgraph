#include "common.h"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t key_range = 1e4;
constexpr size_t op_per_thread = 1e5;
// Depending on value there is a possiblity of numerical overflow
constexpr size_t max_number = 10;
constexpr size_t no_insert_for_one_delete = 1;

// This test checks MultiIterator remove method.
// Each thread removes random data. So removes are joint and scattered on same
// key values.
// Calls of remove method are interleaved with insert calls which always
// succeed.
int main()
{
    init_log();
    memory_check(THREADS_NO, [] {
        multimap_t skiplist;

        auto futures = run<std::pair<long long, std::vector<long long>>>(
            THREADS_NO, skiplist, [](auto acc, auto index) {
                auto rand = rand_gen(key_range);
                auto rand_op = rand_gen_bool(no_insert_for_one_delete);
                long long downcount = op_per_thread;
                std::vector<long long> set(key_range, 0);
                long long sum = 0;

                do {
                    size_t num = rand();
                    auto data = rand() % max_number;
                    if (rand_op()) {

                        int len = 0;
                        for (auto it = acc.find_multi(num); it.has_value();
                             it++) {
                            len++;
                        }
                        if (len > 0) {
                            int pos = rand() % len;
                            for (auto it = acc.find_multi(num); it.has_value();
                                 it++) {
                                if (pos == 0) {
                                    auto data_r = it->second;
                                    if (it.remove()) {
                                        downcount--;
                                        set[num]--;
                                        sum -= data_r;
                                        permanent_assert(
                                            it.is_removed(),
                                            "is_removed method doesn't work");
                                    }
                                    break;
                                }
                                pos--;
                            }
                        }
                    } else {
                        acc.insert(num, data);
                        downcount--;
                        set[num]++;
                        sum += data;
                    }
                } while (downcount > 0);

                return std::pair<long long, std::vector<long long>>(sum, set);
            });

        long set[key_range] = {0};
        long long sums = 0;
        for (auto &data : collect(futures)) {
            sums += data.second.first;
            for (int i = 0; i < key_range; i++) {
                set[i] += data.second.second[i];
            }
        }

        auto accessor = skiplist.access();
        check_multi_iterator(accessor, key_range, set);

        for (auto &e : accessor) {
            set[e.first]--;
            sums -= e.second;
        }
        permanent_assert(sums == 0, "Aproximetly Same values are present");

        check_zero(key_range, set, "MultiMap");

        check_order<multimap_t>(accessor);
    });
}
