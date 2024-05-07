// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <mutex>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"

#include "data_structures/ring_buffer.hpp"
#include "utils/spin_lock.hpp"

TEST(RingBuffer, MultithreadedUsage) {
  auto test_f = [](int producer_count, int elems_per_producer, int producer_sleep_ms, int consumer_count,
                   int consumer_sleep_ms) {
    std::unordered_set<int> consumed;
    memgraph::utils::SpinLock consumed_lock;
    RingBuffer<int> buffer{20};

    std::vector<std::thread> producers;
    for (int i = 0; i < producer_count; i++)
      producers.emplace_back([i, elems_per_producer, producer_sleep_ms, &buffer]() {
        for (int j = 0; j < elems_per_producer; j++) {
          std::this_thread::sleep_for(std::chrono::milliseconds(producer_sleep_ms));
          buffer.emplace(j + i * elems_per_producer);
        }
      });

    std::vector<std::thread> consumers;
    size_t elem_total_count = producer_count * elems_per_producer;
    for (int i = 0; i < consumer_count; i++)
      consumers.emplace_back([elem_total_count, consumer_sleep_ms, &buffer, &consumed, &consumed_lock]() {
        while (true) {
          std::this_thread::sleep_for(std::chrono::milliseconds(consumer_sleep_ms));
          auto guard = std::lock_guard{consumed_lock};
          if (consumed.size() == elem_total_count) break;
          auto value = buffer.pop();
          if (value) consumed.emplace(*value);
        }
      });

    for (auto &producer : producers) producer.join();
    for (auto &consumer : consumers) consumer.join();

    return !buffer.pop() && consumed.size() == elem_total_count;
  };

  // Many slow producers, many fast consumers.
  EXPECT_TRUE(test_f(10, 200, 3, 10, 0));

  // Many fast producers, many slow consumers.
  EXPECT_TRUE(test_f(10, 200, 0, 10, 3));

  // One slower producer, many consumers.
  EXPECT_TRUE(test_f(1, 500, 3, 10, 0));

  // Many producers, one slow consumer.
  EXPECT_TRUE(test_f(10, 200, 0, 1, 3));
}

TEST(RingBuffer, ComplexValues) {
  RingBuffer<std::vector<int>> buffer{10};
  std::vector<int> element;
  for (int i = 0; i < 5; i++) {
    element.emplace_back(i);
    buffer.emplace(element);
  }

  element.clear();
  for (int i = 0; i < 5; i++) {
    element.emplace_back(i);
    EXPECT_EQ(*buffer.pop(), element);
  }

  EXPECT_FALSE(buffer.pop());
}

TEST(RingBuffer, NonCopyable) {
  RingBuffer<std::unique_ptr<std::string>> buffer{10};
  buffer.emplace(new std::string("string"));
  buffer.emplace(new std::string("kifla"));

  EXPECT_EQ(**buffer.pop(), "string");
  EXPECT_EQ(**buffer.pop(), "kifla");
  EXPECT_FALSE(buffer.pop());

  std::unique_ptr<std::string> a(new std::string("bla"));
}
