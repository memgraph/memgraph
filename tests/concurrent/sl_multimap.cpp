#include "common.h"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t key_range = 1e4;
constexpr size_t op_per_thread = 1e5;
// Depending on value there is a possiblity of numerical overflow
constexpr size_t max_number = 10;
constexpr size_t no_insert_for_one_delete = 1;

// This test checks multimap.
// Each thread removes random data. So removes are joint.
// Calls of remove method are interleaved with insert calls which always
// succeed.
int main()
{
    init_log();
    memory_check(THREADS_NO, [] {
        multimap_t skiplist;
        std::atomic<long long> size(0);

        auto futures = run<std::pair<long long, std::vector<long long>>>(
            THREADS_NO, skiplist, [&size](auto acc, auto index) {
                auto rand = rand_gen(key_range);
                auto rand_op = rand_gen_bool(no_insert_for_one_delete);
                long long downcount = op_per_thread;
                std::vector<long long> set(key_range, 0);
                long long sum = 0;

                do {
                    size_t num = rand();
                    auto data = num % max_number;
                    if (rand_op()) {
                        if (acc.remove(num)) {
                            downcount--;
                            set[num]--;
                            sum -= data;
                            size--;
                        }
                    } else {
                        acc.insert(num, data);
                        downcount--;
                        set[num]++;
                        sum += data;
                        size++;
                    }
                } while (downcount > 0);

                return std::pair<long long, std::vector<long long>>(sum, set);
            });

        long set[key_range] = {0};
        long long sums = 0;
        long long size_calc = 0;
        for (auto &data : collect(futures)) {
            sums += data.second.first;
            for (int i = 0; i < key_range; i++) {
                set[i] += data.second.second[i];
                size_calc += data.second.second[i];
            }
        }
        auto accessor = skiplist.access();
        permanent_assert(size == size_calc, "Set size isn't same as counted");
        check_size<multimap_t>(accessor, size);
        check_order<multimap_t>(accessor);

        auto bef_it = accessor.end();
        for (int i = 0; i < key_range; i++) {
            auto it = accessor.find(i);
            if (set[i] > 0) {
                permanent_assert(it != accessor.end(),
                                 "Multimap doesn't contain necessary element "
                                     << i);

                if (bef_it == accessor.end()) bef_it = accessor.find(i);
                for (int j = 0; j < set[i]; j++) {
                    permanent_assert(
                        bef_it != accessor.end(),
                        "Previous iterator doesn't iterate through same "
                        "key entrys. Expected "
                            << i << " for " << set[i] - j
                            << " more times but found null");
                    permanent_assert(
                        bef_it->first == i,
                        "Previous iterator doesn't iterate through same "
                        "key entrys. Expected "
                            << i << " for " << set[i] - j
                            << " more times but found " << bef_it->first
                            << ". Occurances should be " << set[i]);
                    bef_it++;
                }

                for (int j = 0; j < set[i]; j++) {
                    permanent_assert(it != accessor.end(),
                                     "Iterator doesn't iterate through same "
                                     "key entrys. Expected "
                                         << i << " for " << set[i] - j
                                         << " more times but found null");
                    permanent_assert(
                        it->first == i,
                        "Iterator doesn't iterate through same "
                        "key entrys. Expected "
                            << i << " for " << set[i] - j
                            << " more times but found " << it->first
                            << ". Occurances should be " << set[i]);
                    it++;
                }
                permanent_assert(it == accessor.end() || it->first != i,
                                 "There is more data than it should be.");
                bef_it = it;
            }
        }

        for (auto &e : accessor) {
            set[e.first]--;
            sums -= e.second;
        }
        permanent_assert(sums == 0, "Aproximetly Same values are present");

        check_zero(key_range, set, "MultiMap");
    });
}
