#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include <chrono>
#include <future>
#include <iostream>
#include <random>
#include <thread>

#include "data_structures/bitset/dynamic_bitset.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/concurrent_multimap.hpp"
#include "data_structures/concurrent/concurrent_multiset.hpp"
#include "data_structures/concurrent/concurrent_set.hpp"
#include "data_structures/concurrent/skiplist.hpp"
#include "data_structures/concurrent/concurrent_list.hpp"
#include "data_structures/static_array.hpp"
#include "utils/assert.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/sysinfo/memory.hpp"

using std::cout;
using std::endl;
using map_t = ConcurrentMap<int, int>;
using set_t = ConcurrentSet<int>;
using multiset_t = ConcurrentMultiSet<int>;
using multimap_t = ConcurrentMultiMap<int, int>;

using namespace std::chrono_literals;

// Returns uniform random size_t generator from range [0,n>
auto rand_gen(size_t n)
{
    std::default_random_engine generator;
    std::uniform_int_distribution<size_t> distribution(0, n - 1);
    return std::bind(distribution, generator);
}

// Returns random bool generator with distribution of 1 true for n false.
auto rand_gen_bool(size_t n = 1)
{
    auto gen = rand_gen(n + 1);
    return [=]() mutable { return gen() == 0; };
}

// Checks for all owned keys if there data is data.
template <typename S>
void check_present_same(typename S::Accessor &acc, size_t data,
                        std::vector<size_t> &owned)
{
    for (auto num : owned) {
        permanent_assert(acc.find(num)->second == data,
                         "My data is present and my");
    }
}

// Checks for all owned.second keys if there data is owned.first.
template <typename S>
void check_present_same(typename S::Accessor &acc,
                        std::pair<size_t, std::vector<size_t>> &owned)
{
    check_present_same<S>(acc, owned.first, owned.second);
}

// Checks if reported size and traversed size are equal to given size.
template <typename S>
void check_size_list(S &acc, long long size)
{
    // check size

    permanent_assert(acc.size() == size, "Size should be " << size
                                                           << ", but size is "
                                                           << acc.size());

    // check count

    size_t iterator_counter = 0;

    for (auto elem : acc) {
        ++iterator_counter;
    }
    permanent_assert(iterator_counter == size, "Iterator count should be "
                                                   << size << ", but size is "
                                                   << iterator_counter);
}
template <typename S>
void check_size(typename S::Accessor &acc, long long size)
{
    // check size

    permanent_assert(acc.size() == size, "Size should be " << size
                                                           << ", but size is "
                                                           << acc.size());

    // check count

    size_t iterator_counter = 0;

    for (auto elem : acc) {
        ++iterator_counter;
    }
    permanent_assert(iterator_counter == size, "Iterator count should be "
                                                   << size << ", but size is "
                                                   << iterator_counter);
}

// Checks if order in list is maintened. It expects map
template <typename S>
void check_order(typename S::Accessor &acc)
{
    if (acc.begin() != acc.end()) {
        auto last = acc.begin()->first;
        for (auto elem : acc) {
            if (!(last <= elem))
                std::cout << "Order isn't maintained. Before was: " << last
                          << " next is " << elem.first << "\n";
            last = elem.first;
        }
    }
}

void check_zero(size_t key_range, long array[], const char *str)
{
    for (int i = 0; i < key_range; i++) {
        permanent_assert(array[i] == 0,
                         str << " doesn't hold it's guarantees. It has "
                             << array[i] << " extra elements.");
    }
}

void check_set(DynamicBitset<> &db, std::vector<bool> &set)
{
    for (int i = 0; i < set.size(); i++) {
        permanent_assert(!(set[i] ^ db.at(i)),
                         "Set constraints aren't fullfilled.");
    }
}

// Checks multiIterator and iterator guarantees
void check_multi_iterator(multimap_t::Accessor &accessor, size_t key_range,
                          long set[])
{
    for (int i = 0; i < key_range; i++) {
        auto it = accessor.find(i);
        auto it_m = accessor.find_multi(i);
        permanent_assert(
            !(it_m != accessor.end(i) && it == accessor.end()),
            "MultiIterator ended before Iterator. Set: " << set[i]);
        permanent_assert(
            !(it_m == accessor.end(i) && it != accessor.end()),
            "Iterator ended before MultiIterator. Set: " << set[i]);
        permanent_assert((it_m == accessor.end(i) && it == accessor.end()) ||
                             it->second == it_m->second,
                         "MultiIterator didn't found the same "
                         "first element. Set: "
                             << set[i]);
        if (set[i] > 0) {
            for (int j = 0; j < set[i]; j++) {
                permanent_assert(
                    it->second == it_m->second,
                    "MultiIterator and iterator aren't on the same "
                    "element.");
                permanent_assert(it_m->first == i,
                                 "MultiIterator is showing illegal data") it++;
                it_m++;
            }
        }
        permanent_assert(
            it_m == accessor.end(i),
            "There is more data than it should be in MultiIterator. "
                << it_m->first << "\n");
    }
}

// Runs given function in threads_no threads and returns vector of futures for
// there
// results.
template <class R, typename S>
std::vector<std::future<std::pair<size_t, R>>>
run(size_t threads_no, S &skiplist,
    std::function<R(typename S::Accessor, size_t)> f)
{
    std::vector<std::future<std::pair<size_t, R>>> futures;

    for (size_t thread_i = 0; thread_i < threads_no; ++thread_i) {
        std::packaged_task<std::pair<size_t, R>()> task(
            [&skiplist, f, thread_i]() {
                return std::pair<size_t, R>(thread_i,
                                            f(skiplist.access(), thread_i));
            });                               // wrap the function
        futures.push_back(task.get_future()); // get a future
        std::thread(std::move(task)).detach();
    }
    return futures;
}

// Runs given function in threads_no threads and returns vector of futures for
// there
// results.
template <class R>
std::vector<std::future<std::pair<size_t, R>>> run(size_t threads_no,
                                                   std::function<R(size_t)> f)
{
    std::vector<std::future<std::pair<size_t, R>>> futures;

    for (size_t thread_i = 0; thread_i < threads_no; ++thread_i) {
        std::packaged_task<std::pair<size_t, R>()> task([f, thread_i]() {
            return std::pair<size_t, R>(thread_i, f(thread_i));
        });                                   // wrap the function
        futures.push_back(task.get_future()); // get a future
        std::thread(std::move(task)).detach();
    }
    return futures;
}

// Collects all data from futures.
template <class R>
auto collect(std::vector<std::future<R>> &collect)
{
    std::vector<R> collection;
    for (auto &fut : collect) {
        collection.push_back(fut.get());
    }
    return collection;
}

std::vector<bool> collect_set(
    std::vector<std::future<std::pair<size_t, std::vector<bool>>>> &&futures)
{
    std::vector<bool> set;
    for (auto &data : collect(futures)) {
        set.resize(data.second.size());
        for (int i = 0; i < data.second.size(); i++) {
            set[i] = set[i] | data.second[i];
        }
    }
    return set;
}

// Returns object which tracs in owned which (key,data) where added and
// downcounts.
template <class K, class D, class S>
auto insert_try(typename S::Accessor &acc, long long &downcount,
                std::vector<K> &owned)
{
    return [&](K key, D data) mutable {
        if (acc.insert(key, data).second) {
            downcount--;
            owned.push_back(key);
        }
    };
}

// Helper function.
int parseLine(char *line)
{
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char *p = line;
    while (*p < '0' || *p > '9')
        p++;
    line[i - 3] = '\0';
    i = atoi(p);
    return i;
}

// Returns currentlz used memory in kB.
int currently_used_memory()
{ // Note: this value is in KB!
    FILE *file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL) {
        if (strncmp(line, "VmSize:", 7) == 0) {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

// Performs memory check to determine if memory usage before calling given
// function
// is aproximately equal to memory usage after function. Memory usage is thread
// senstive so no_threads spawned in function is necessary.
void memory_check(size_t no_threads, std::function<void()> f)
{
    long long start = currently_used_memory();
    f();
    long long leaked =
        currently_used_memory() - start -
        no_threads * 73732; // OS sensitive, 73732 size allocated for thread
    std::cout << "leaked: " << leaked << "\n";
    permanent_assert(leaked <= 0, "Memory leak check");
}

//Initializes loging faccilityes
void init_log(){
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());
}
