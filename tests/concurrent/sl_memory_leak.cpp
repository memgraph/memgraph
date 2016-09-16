#include "common.h"

constexpr size_t THREADS_NO = std::min(max_no_threads, 1);
constexpr size_t elems_per_thread = 16e5;

// Known memory leak at 1,600,000 elements.
int main()
{
    init_log();
    memory_check(THREADS_NO, [&] {
        ds::static_array<std::thread, THREADS_NO> threads;
        map_t skiplist;

        // put THREADS_NO * elems_per_thread items to the skiplist
        for (size_t thread_i = 0; thread_i < THREADS_NO; ++thread_i) {
            threads[thread_i] = std::thread(
                [&skiplist](size_t start, size_t end) {
                    auto accessor = skiplist.access();
                    for (size_t elem_i = start; elem_i < end; ++elem_i) {
                        accessor.insert(elem_i, elem_i);
                    }
                },
                thread_i * elems_per_thread,
                thread_i * elems_per_thread + elems_per_thread);
        }
        // wait all threads
        for (auto &thread : threads) {
            thread.join();
        }

        // get skiplist size
        {
            auto accessor = skiplist.access();
            permanent_assert(accessor.size() == THREADS_NO * elems_per_thread,
                             "all elements in skiplist");
        }

        for (size_t thread_i = 0; thread_i < THREADS_NO; ++thread_i) {
            threads[thread_i] = std::thread(
                [&skiplist](size_t start, size_t end) {
                    auto accessor = skiplist.access();
                    for (size_t elem_i = start; elem_i < end; ++elem_i) {
                        permanent_assert(accessor.remove(elem_i) == true, "");
                    }
                },
                thread_i * elems_per_thread,
                thread_i * elems_per_thread + elems_per_thread);
        }
        // // wait all threads
        for (auto &thread : threads) {
            thread.join();
        }

        // check size
        {
            auto accessor = skiplist.access();
            permanent_assert(accessor.size() == 0,
                             "Size should be 0, but size is "
                                 << accessor.size());
        }

        // check count
        {
            size_t iterator_counter = 0;
            auto accessor = skiplist.access();
            for (auto elem : accessor) {
                ++iterator_counter;
                cout << elem.first << " ";
            }
            permanent_assert(iterator_counter == 0, "deleted elements");
        }
    });
}
