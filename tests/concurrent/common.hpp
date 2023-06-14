// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <future>
#include <iostream>
#include <random>
#include <thread>

#include "data_structures/bitset/dynamic_bitset.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/skiplist.hpp"
#include "utils/logging.hpp"

// NOTE: this file is highly coupled to data_structures
// TODO: REFACTOR

// Sets max number of threads that will be used in concurrent tests.
inline constexpr int max_no_threads = 8;

using std::cout;
using std::endl;
using map_t = ConcurrentMap<int, int>;

using namespace std::chrono_literals;

// Returns uniform random size_t generator from range [0,n>
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

// Checks for all owned keys if their data is data.
template <typename TAccessor>
void check_present_same(TAccessor &acc, size_t data, std::vector<size_t> &owned) {
  for (auto num : owned) {
    MG_ASSERT(acc.find(num)->second == data, "My data is present and my");
  }
}

// Checks for all owned.second keys if their data is owned.first.
template <typename TAccessor>
void check_present_same(TAccessor &acc, std::pair<size_t, std::vector<size_t>> &owned) {
  check_present_same(acc, owned.first, owned.second);
}

// Checks if reported size and traversed size are equal to given size.
template <typename TAccessor>
void check_size_list(TAccessor &acc, long long size) {
  // check size

  MG_ASSERT(acc.size() == size, "Size should be {}, but size is {}", size, acc.size());

  // check count

  size_t iterator_counter = 0;

  for ([[gnu::unused]] auto elem : acc) {
    ++iterator_counter;
  }
  MG_ASSERT(static_cast<int64_t>(iterator_counter) == size, "Iterator count should be {}, but size is {}", size,
            iterator_counter);
}
template <typename TAccessor>
void check_size(TAccessor &acc, long long size) {
  // check size

  MG_ASSERT(acc.size() == size, "Size should be {}, but size is {}", size, acc.size());

  // check count

  size_t iterator_counter = 0;

  for ([[gnu::unused]] auto elem : acc) {
    ++iterator_counter;
  }
  MG_ASSERT(static_cast<int64_t>(iterator_counter) == size)
      << "Iterator count should be " << size << ", but size is " << iterator_counter;
}

// Checks if order in list is maintained. It expects map
template <typename TAccessor>
void check_order(TAccessor &acc) {
  if (acc.begin() != acc.end()) {
    auto last = acc.begin()->first;
    for (auto elem : acc) {
      if (!(last <= elem))
        std::cout << "Order isn't maintained. Before was: " << last << " next is " << elem.first << "\n";
      last = elem.first;
    }
  }
}

void check_zero(size_t key_range, long array[], const char *str) {
  for (int i = 0; i < static_cast<int>(key_range); i++) {
    MG_ASSERT(array[i] == 0, "{} doesn't hold it's guarantees. It has {} extra elements.", str, array[i]);
  }
}

void check_set(DynamicBitset<> &db, std::vector<bool> &set) {
  for (int i = 0; i < static_cast<int>(set.size()); i++) {
    MG_ASSERT(!(set[i] ^ db.at(i)), "Set constraints aren't fulfilled.");
  }
}

// Runs given function in threads_no threads and returns vector of futures for
// their results.
template <class R, typename S, class FunT>
std::vector<std::future<std::pair<size_t, R>>> run(size_t threads_no, S &skiplist, FunT f) {
  std::vector<std::future<std::pair<size_t, R>>> futures;

  for (size_t thread_i = 0; thread_i < threads_no; ++thread_i) {
    std::packaged_task<std::pair<size_t, R>()> task([&skiplist, f, thread_i]() {
      return std::pair<size_t, R>(thread_i, f(skiplist.access(), thread_i));
    });                                    // wrap the function
    futures.push_back(task.get_future());  // get a future
    std::thread(std::move(task)).detach();
  }
  return futures;
}

// Runs given function in threads_no threads and returns vector of futures for
// their results.
template <class R>
std::vector<std::future<std::pair<size_t, R>>> run(size_t threads_no, std::function<R(size_t)> f) {
  std::vector<std::future<std::pair<size_t, R>>> futures;

  for (size_t thread_i = 0; thread_i < threads_no; ++thread_i) {
    std::packaged_task<std::pair<size_t, R>()> task(
        [f, thread_i]() { return std::pair<size_t, R>(thread_i, f(thread_i)); });  // wrap the function
    futures.push_back(task.get_future());                                          // get a future
    std::thread(std::move(task)).detach();
  }
  return futures;
}

// Collects all data from futures.
template <class R>
auto collect(std::vector<std::future<R>> &collect) {
  std::vector<R> collection;
  for (auto &fut : collect) {
    collection.push_back(fut.get());
  }
  return collection;
}

std::vector<bool> collect_set(std::vector<std::future<std::pair<size_t, std::vector<bool>>>> &&futures) {
  std::vector<bool> set;
  for (auto &data : collect(futures)) {
    set.resize(data.second.size());
    for (int i = 0; i < static_cast<int>(data.second.size()); i++) {
      set[i] = set[i] | data.second[i];
    }
  }
  return set;
}
