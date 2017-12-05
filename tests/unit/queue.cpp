#include <atomic>
#include <chrono>
#include <experimental/optional>
#include <string>
#include <thread>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "data_structures/queue.hpp"

namespace {

using namespace std::literals::chrono_literals;

TEST(Queue, PushMaybePop) {
  Queue<int> q;
  q.Push(1);
  EXPECT_EQ(*q.MaybePop(), 1);
  EXPECT_EQ(q.MaybePop(), std::experimental::nullopt);

  q.Push(2);
  q.Push(3);
  EXPECT_EQ(*q.MaybePop(), 2);

  q.Push(4);
  q.Push(5);
  EXPECT_EQ(*q.MaybePop(), 3);
  EXPECT_EQ(*q.MaybePop(), 4);
  EXPECT_EQ(*q.MaybePop(), 5);
  EXPECT_EQ(q.MaybePop(), std::experimental::nullopt);
}

TEST(Queue, Emplace) {
  Queue<std::pair<std::string, int>> q;
  q.Emplace("abc", 123);
  EXPECT_THAT(*q.MaybePop(), testing::Pair("abc", 123));
}

TEST(Queue, Size) {
  Queue<int> q;
  EXPECT_EQ(q.size(), 0);

  q.Push(1);
  EXPECT_EQ(q.size(), 1);

  q.Push(1);
  EXPECT_EQ(q.size(), 2);

  q.MaybePop();
  EXPECT_EQ(q.size(), 1);

  q.MaybePop();
  EXPECT_EQ(q.size(), 0);
  q.MaybePop();
  EXPECT_EQ(q.size(), 0);
}

TEST(Queue, Empty) {
  Queue<int> q;
  EXPECT_TRUE(q.empty());

  q.Push(1);
  EXPECT_FALSE(q.empty());

  q.MaybePop();
  EXPECT_TRUE(q.empty());
}

TEST(Queue, AwaitPop) {
  Queue<int> q;
  std::thread t([&] {
    q.Push(1);
    q.Push(2);
    std::this_thread::sleep_for(200ms);
    q.Push(3);
    q.Push(4);
  });

  EXPECT_EQ(*q.AwaitPop(), 1);
  std::this_thread::sleep_for(1000ms);
  EXPECT_EQ(*q.AwaitPop(), 2);
  EXPECT_EQ(*q.AwaitPop(), 3);
  EXPECT_EQ(*q.AwaitPop(), 4);
  t.join();

  std::thread t2([&] {
    std::this_thread::sleep_for(100ms);
    q.Shutdown();
  });
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(q.AwaitPop(), std::experimental::nullopt);
  t2.join();
}

TEST(Queue, AwaitPopTimeout) {
  std::this_thread::sleep_for(1000ms);
  Queue<int> q;
  EXPECT_EQ(q.AwaitPop(100ms), std::experimental::nullopt);
}

TEST(Queue, Concurrent) {
  Queue<int> q;

  const int kNumProducers = 10;
  const int kNumConsumers = 10;
  const int kNumElementsPerProducer = 300000;

  std::vector<std::thread> producers;
  std::atomic<int> next{0};

  for (int i = 0; i < kNumProducers; ++i) {
    producers.emplace_back([&] {
      for (int i = 0; i < kNumElementsPerProducer; ++i) {
        q.Push(next++);
      }
    });
  }

  std::vector<std::thread> consumers;
  std::vector<int> retrieved[kNumConsumers];
  std::atomic<int> num_retrieved{0};
  for (int i = 0; i < kNumConsumers; ++i) {
    consumers.emplace_back(
        [&](int thread_id) {
          while (true) {
            int count = num_retrieved++;
            if (count >= kNumProducers * kNumElementsPerProducer) break;
            retrieved[thread_id].push_back(*q.AwaitPop());
          }
        },
        i);
  }

  for (auto &t : consumers) {
    t.join();
  }
  for (auto &t : producers) {
    t.join();
  }

  EXPECT_EQ(q.MaybePop(), std::experimental::nullopt);

  std::set<int> all_elements;
  for (auto &r : retrieved) {
    all_elements.insert(r.begin(), r.end());
  }
  EXPECT_EQ(all_elements.size(), kNumProducers * kNumElementsPerProducer);
  EXPECT_EQ(*all_elements.begin(), 0);
  EXPECT_EQ(*all_elements.rbegin(),
            kNumProducers * kNumElementsPerProducer - 1);
}
}
