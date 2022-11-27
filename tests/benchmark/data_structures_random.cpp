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

#include <gflags/gflags.h>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <stdexcept>
#include <type_traits>

#include "btree_map.hpp"
#include "skip_list_common.hpp"
#include "utils/skip_list.hpp"

DEFINE_int32(max_element, 20000, "Maximum element in the intial list");
DEFINE_int32(max_range, 2000000, "Maximum range used for the test");
DEFINE_string(ds, "skip_list", "Which DS to test");

template <class ContainerType>
concept BasicIndexStructure = requires(ContainerType a, const ContainerType b) {
  { a.contains() } -> std::same_as<bool>;
  { a.find() } -> std::same_as<typename ContainerType::iterator>;
  { a.insert() } -> std::same_as<std::pair<typename ContainerType::iterator, bool>>;
  { a.remove() } -> std::same_as<bool>;
};

template <typename T>
class BppStructure final {
  using value_type = typename tlx::btree_map<T, T>::value_type;
  using iterator = typename tlx::btree_map<T, T>::iterator;

 public:
  bool contains(const T key) const { return data.exists(key); }

  auto find(const T key) const { return data.find(key); }

  std::pair<iterator, bool> insert(T val) { return data.insert({val, val}); }

  bool remove(const T key) { return data.erase(key); }

  size_t size() const { return data.size(); };

  iterator end() { return data.end(); }

 private:
  tlx::btree_map<T, T> data;
};

template <typename T>
class MapStructure final {
  using value_type = typename std::map<T, T>::value_type;
  using iterator = typename std::map<T, T>::iterator;

 public:
  bool contains(const T key) const { return data.contains(key); }

  auto find(const T key) const { return data.find(key); }

  std::pair<iterator, bool> insert(T val) { return data.insert({val, val}); }

  bool remove(const T key) { return data.erase(key); }

  size_t size() const { return data.size(); };

  iterator end() { return data.end(); }

 private:
  std::map<T, T> data;
};

template <typename DataStructure>
void RunDSTest(DataStructure &ds) {
  RunTest([&ds](const std::atomic<bool> &run, auto &stats) {
    std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distribution(0, 3);
    std::mt19937 i_generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> i_distribution(0, FLAGS_max_range);

    while (run.load(std::memory_order_relaxed)) {
      auto value = distribution(generator);

      auto item = i_distribution(i_generator);
      switch (value) {
        case 0:
          stats.succ[OP_INSERT] += static_cast<uint64_t>(ds.insert(item).second);
          break;
        case 1:
          stats.succ[OP_CONTAINS] += static_cast<uint64_t>(ds.contains(item));
          break;
        case 2:
          stats.succ[OP_REMOVE] += static_cast<uint64_t>(ds.remove(item));
          break;
        case 3:
          stats.succ[OP_FIND] += static_cast<uint64_t>(ds.find(item) != ds.end());
          break;
        default:
          std::terminate();
      }
      ++stats.total;
    }
  });
}

template <typename Accessor>
void GenerateData(Accessor &acc) {
  for (uint64_t i = 0; i <= FLAGS_max_element; ++i) {
    MG_ASSERT(acc.insert(i).second);
  }
}

template <typename Accessor>
void AsserData(Accessor &acc) {
  if constexpr (std::is_same_v<Accessor, memgraph::utils::SkipList<uint64_t>>) {
    uint64_t val = 0;
    for (auto item : acc) {
      MG_ASSERT(item == val);
      ++val;
    }
    MG_ASSERT(val == FLAGS_max_element + 1);
  } else {
    MG_ASSERT(acc.size() == FLAGS_max_element + 1);
  }
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_ds == "skip_list") {
    std::cout << "#### Testing skip list" << std::endl;
    memgraph::utils::SkipList<uint64_t> list;
    auto acc = list.access();

    GenerateData(acc);
    AsserData(acc);

    RunDSTest(acc);
  } else if (FLAGS_ds == "map") {
    std::cout << "#### Testing map" << std::endl;
    MapStructure<uint64_t> map;

    GenerateData(map);
    AsserData(map);

    RunDSTest(map);
  } else if (FLAGS_ds == "bpp") {
    std::cout << "#### Testing B+ tree" << std::endl;
    BppStructure<uint64_t> bpp;

    GenerateData(bpp);
    AsserData(bpp);

    RunDSTest(bpp);
  } else {
    throw std::runtime_error("Select an existing data structure!");
  }

  return 0;
}
