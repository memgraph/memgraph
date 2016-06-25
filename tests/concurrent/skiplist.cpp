#include <chrono>
#include <iostream>
#include <thread>

#include "data_structures/skiplist/skiplist.hpp"
#include "data_structures/static_array.hpp"
#include "utils/assert.hpp"
#include "utils/sysinfo/memory.hpp"

using std::cout;
using std::endl;
using skiplist_t = SkipList<int, int>;
using namespace std::chrono_literals;

#define THREADS_NO 16
constexpr size_t elems_per_thread = 100000;

int main()
{
    ds::static_array<std::thread, THREADS_NO> threads;
    skiplist_t skiplist;
    
    cout << "1. used virtual memory: " << used_virtual_memory() << endl;

    // put THREADS_NO * elems_per_thread items to the skiplist
    for (size_t thread_i = 0; thread_i < THREADS_NO; ++thread_i) {
        threads[thread_i] = std::thread(
            [&skiplist](size_t start, size_t end) {
                auto accessor = skiplist.access();
                for (size_t elem_i = start; elem_i < end; ++elem_i) {
                    accessor.insert_unique(elem_i, elem_i);
                }
            },
            thread_i * elems_per_thread,
            thread_i * elems_per_thread + elems_per_thread);
    }

    // wait all threads
    for (auto &thread : threads) {
        thread.join();
    }

    cout << "1. used virtual memory: " << used_virtual_memory() << endl;

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

    // wait all threads
    for (auto &thread : threads) {
        thread.join();
    }

    cout << "1. used virtual memory: " << used_virtual_memory() << endl;

    // check size
    {
        auto accessor = skiplist.access();
        permanent_assert(accessor.size() == 0, "Size should be 0, but size is " << accessor.size());
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

    // TODO: test GC and memory
    std::this_thread::sleep_for(5s);
    cout << "1. used virtual memory: " << used_virtual_memory() << endl;

    return 0;
}
