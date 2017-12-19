#include <map>
#include <thread>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "database/graph_db_datatypes.hpp"
#include "storage/concurrent_id_mapper_single_node.hpp"

using Id = GraphDbTypes::Label;
using Mapper = storage::SingleNodeConcurrentIdMapper<Id>;

TEST(ConcurrentIdMapper, SameValueGivesSameId) {
  Mapper mapper;
  EXPECT_EQ(mapper.value_to_id("a"), mapper.value_to_id("a"));
}

TEST(ConcurrentIdMapper, IdToValue) {
  Mapper mapper;
  std::string value = "a";
  auto id = mapper.value_to_id(value);
  EXPECT_EQ(value, mapper.id_to_value(id));
}

TEST(ConcurrentIdMapper, TwoValuesTwoIds) {
  Mapper mapper;
  EXPECT_NE(mapper.value_to_id("a"), mapper.value_to_id("b"));
}

TEST(ConcurrentIdMapper, SameIdReturnedMultipleThreads) {
  const int thread_count = 20;
  std::vector<std::string> values;
  for (int i = 0; i < 50; ++i) values.emplace_back("value" + std::to_string(i));

  // Perform the whole test a number of times since it's stochastic (we're
  // trying to detect bad behavior in parallel execution).
  for (int loop_ind = 0; loop_ind < 20; ++loop_ind) {
    Mapper mapper;
    std::vector<std::map<Id, std::string>> mappings(thread_count);
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
