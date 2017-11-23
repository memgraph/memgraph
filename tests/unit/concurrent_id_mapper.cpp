#include <thread>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "storage/concurrent_id_mapper.hpp"

const int THREAD_NUM = 20;
const int VALUE_MAX = 50;

TEST(ConcurrentIdMapper, SameValueGivesSameId) {
  ConcurrentIdMapper<int, int, int> mapper;
  EXPECT_EQ(mapper.insert_value(1), mapper.insert_value(1));
}

TEST(ConcurrentIdMapper, IdToValue) {
  ConcurrentIdMapper<int, int, int> mapper;
  auto value = 1;
  auto id = mapper.insert_value(value);
  EXPECT_EQ(value, mapper.value_by_id(id));
}

TEST(ConcurrentIdMapper, TwoValuesTwoIds) {
  ConcurrentIdMapper<int, int, int> mapper;
  EXPECT_NE(mapper.insert_value(1), mapper.insert_value(2));
}

TEST(ConcurrentIdMapper, SameIdReturnedMultipleThreads) {
  std::vector<std::thread> threads;
  ConcurrentIdMapper<int, int, int> mapper;
  std::vector<std::vector<int>> thread_value_ids(THREAD_NUM);

  std::atomic<int> current_value{0};
  std::atomic<int> current_value_insertion_count{0};

  // Try to insert every value from [0, VALUE_MAX] by multiple threads in the
  // same time
  for (int i = 0; i < THREAD_NUM; ++i) {
    threads.push_back(std::thread([&mapper, &thread_value_ids, &current_value,
                                   &current_value_insertion_count, i]() {
      int last = -1;
      while (current_value <= VALUE_MAX) {
        while (last == current_value) continue;
        auto id = mapper.insert_value(current_value.load());
        thread_value_ids[i].push_back(id);
        // Also check that reverse mapping exists after method exits
        EXPECT_EQ(mapper.value_by_id(id), current_value.load());
        last = current_value;
        current_value_insertion_count.fetch_add(1);
      }
    }));
  }
  // Increment current_value when all threads finish inserting it and getting an
  // id for it
  threads.push_back(
      std::thread([&current_value, &current_value_insertion_count]() {
        while (current_value.load() <= VALUE_MAX) {
          while (current_value_insertion_count.load() != THREAD_NUM) continue;
          current_value_insertion_count.store(0);
          current_value.fetch_add(1);
        }
      }));
  for (auto &thread : threads) thread.join();

  // For every value inserted, each thread should have the same id
  for (int i = 0; i < THREAD_NUM; ++i)
    for (int j = 0; j < THREAD_NUM; ++j)
      EXPECT_EQ(thread_value_ids[i], thread_value_ids[j]);

  // Each value should have a unique id
  std::set<int> ids(thread_value_ids[0].begin(), thread_value_ids[0].end());
  EXPECT_EQ(ids.size(), thread_value_ids[0].size());
}
