#include <map>
#include <thread>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "storage/concurrent_id_mapper_single_node.hpp"
#include "storage/types.hpp"

using IdLabel = storage::Label;
using MapperLabel = storage::SingleNodeConcurrentIdMapper<IdLabel>;

TEST(ConcurrentIdMapper, SameValueGivesSameId) {
  MapperLabel mapper;
  EXPECT_EQ(mapper.value_to_id("a"), mapper.value_to_id("a"));
}

TEST(ConcurrentIdMapper, IdToValue) {
  MapperLabel mapper;
  std::string value = "a";
  auto id = mapper.value_to_id(value);
  EXPECT_EQ(value, mapper.id_to_value(id));
}

TEST(ConcurrentIdMapper, TwoValuesTwoIds) {
  MapperLabel mapper;
  EXPECT_NE(mapper.value_to_id("a"), mapper.value_to_id("b"));
}

TEST(ConcurrentIdMapper, SameIdReturnedMultipleThreads) {
  const int thread_count = 20;
  std::vector<std::string> values;
  for (int i = 0; i < 50; ++i) values.emplace_back("value" + std::to_string(i));

  // Perform the whole test a number of times since it's stochastic (we're
  // trying to detect bad behavior in parallel execution).
  for (int loop_ind = 0; loop_ind < 20; ++loop_ind) {
    MapperLabel mapper;
    std::vector<std::map<IdLabel, std::string>> mappings(thread_count);
    std::vector<std::thread> threads;
    for (int thread_ind = 0; thread_ind < thread_count; ++thread_ind) {
      threads.emplace_back([&mapper, &mappings, &values, thread_ind] {
        auto &mapping = mappings[thread_ind];
        for (auto &value : values) {
          mapping.emplace(mapper.value_to_id(value), value);
        }
      });
    }
    for (auto &thread : threads) thread.join();
    EXPECT_EQ(mappings[0].size(), values.size());
    for (auto &mapping : mappings) EXPECT_EQ(mapping, mappings[0]);
  }
}

using IdProperty = storage::Property;
using MapperProperty = storage::SingleNodeConcurrentIdMapper<IdProperty>;

TEST(ConcurrentIdMapper, PropertyLocation) {
  // TODO(ipaljak): write unit tests for storage::Common and all
  // derived classes (tests/unit/storage_types.cpp)
  std::string prop_on_disk_name = "test_name1";
  std::string prop_in_mem_name = "test_name2";
  std::vector<std::string> props_on_disk = {prop_on_disk_name};
  MapperProperty mapper(props_on_disk);

  auto on_disk = mapper.value_to_id(prop_on_disk_name);
  ASSERT_EQ(on_disk.Id(), 0);
  ASSERT_EQ(on_disk.Location(), storage::Location::Disk);

  auto in_mem = mapper.value_to_id(prop_in_mem_name);
  ASSERT_EQ(in_mem.Id(), 1);
  ASSERT_EQ(in_mem.Location(), storage::Location::Memory);
}
