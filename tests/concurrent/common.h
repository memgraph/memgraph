#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include <chrono>
#include <future>
#include <iostream>
#include <random>
#include <thread>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/skiplist.hpp"
#include "data_structures/static_array.hpp"
#include "utils/assert.hpp"
#include "utils/sysinfo/memory.hpp"

using std::cout;
using std::endl;
using skiplist_t = ConcurrentMap<int, int>;
using namespace std::chrono_literals;

auto rand_gen(size_t n) {
  std::default_random_engine generator;
  std::uniform_int_distribution<size_t> distribution(0, n - 1);
  return std::bind(distribution, generator);
}

// Returns random bool generator with distribution of 1 true for n false.
auto rand_gen_bool(size_t n = 1) {
  auto gen = rand_gen(n + 1);
  return [=]() mutable { return gen() == 0; };
}

void check_present_same(skiplist_t::Accessor &acc,
                        std::pair<size_t, std::vector<size_t>> &owned) {
  for (auto num : owned.second) {
    permanent_assert(acc.find(num)->second == owned.first,
                     "My data is present and my");
  }
}
void check_present_same(skiplist_t::Accessor &acc, size_t owner,
                        std::vector<size_t> &owned) {
  for (auto num : owned) {
    permanent_assert(acc.find(num)->second == owner,
                     "My data is present and my");
  }
}

void check_size(const skiplist_t::Accessor &acc, long long size) {
  // check size

  permanent_assert(acc.size() == size,
                   "Size should be " << size << ", but size is " << acc.size());

  // check count

  size_t iterator_counter = 0;

  for (auto elem : acc) {
    ++iterator_counter;
  }
  permanent_assert(iterator_counter == size, "Iterator count should be "
                                                 << size << ", but size is "
                                                 << acc.size());
}

template <class R>
std::vector<std::future<std::pair<size_t, R>>>
run(size_t threads_no, skiplist_t &skiplist,
    std::function<R(skiplist_t::Accessor, size_t)> f) {
  std::vector<std::future<std::pair<size_t, R>>> futures;

  for (size_t thread_i = 0; thread_i < threads_no; ++thread_i) {
    std::packaged_task<std::pair<size_t, R>()> task([&skiplist, f, thread_i]() {
      return std::pair<size_t, R>(thread_i, f(skiplist.access(), thread_i));
    });                                   // wrap the function
    futures.push_back(task.get_future()); // get a future
    std::thread(std::move(task)).detach();
  }
  return futures;
}

template <class R> auto collect(std::vector<std::future<R>> &collect) {
  std::vector<R> collection;
  for (auto &fut : collect) {
    collection.push_back(fut.get());
  }
  return collection;
}

template <class K, class D>
auto insert_try(skiplist_t::Accessor &acc, size_t &downcount,
                std::vector<K> &owned) {
  return [&](K key, D data) mutable {
    if (acc.insert(key, data).second) {
      downcount--;
      owned.push_back(key);
    }
  };
}

int parseLine(char *line) {
  // This assumes that a digit will be found and the line ends in " Kb".
  int i = strlen(line);
  const char *p = line;
  while (*p < '0' || *p > '9')
    p++;
  line[i - 3] = '\0';
  i = atoi(p);
  return i;
}

int currently_used_memory() { // Note: this value is in KB!
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

void memory_check(size_t no_threads, std::function<void()> f) {
  long long start = currently_used_memory();
  f();
  long long leaked =
      currently_used_memory() - start -
      no_threads * 73732; // OS sensitive, 73732 size allocated for thread
  std::cout << "leaked: " << leaked << "\n";
  permanent_assert(leaked <= 0, "Memory leak check");
}
