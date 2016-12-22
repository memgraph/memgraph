#include "common.h"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t elements = 2e6;

/**
 * Put elements number of elements in the skiplist per each thread and see
 * is there any memory leak
 */
int main()
{
    init_log();

    memory_check(THREADS_NO, [] {
        map_t skiplist;

        auto futures =
            run<size_t>(THREADS_NO, skiplist, [](auto acc, auto index) {
                for (size_t i = 0; i < elements; i++) {
                    acc.insert(i, index);
                }
                return index;
            });
        collect(futures);

        auto accessor = skiplist.access();
        check_size<map_t>(accessor, elements);
    });
}
