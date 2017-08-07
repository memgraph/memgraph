#include <thread>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "data_structures/concurrent/push_queue.hpp"

class IntQueue : public ::testing::Test {
 protected:
  ConcurrentPushQueue<int> cpq;

  void AddElems(int count) {
    for (int i = 0; i < count; i++) cpq.push(i);
  }

  int CountIterations() {
    int rval = 0;
    for ([[gnu::unused]] int x : cpq) rval++;
    return rval;
  }
};

TEST_F(IntQueue, EmptyQueueAndPush) {
  ConcurrentPushQueue<int> cpq;
  EXPECT_EQ(cpq.size(), 0);
  cpq.push(1);
  EXPECT_EQ(cpq.size(), 1);
  cpq.push(1);
  EXPECT_EQ(cpq.size(), 2);
}

TEST_F(IntQueue, Size) {
  AddElems(10);
  EXPECT_EQ(cpq.size(), 10);
}

TEST_F(IntQueue, ConcurrentPush) {
  // perform 1000 insertions in 50 concurrent threads
  std::vector<std::thread> threads;
  for (int i = 0; i < 50; i++)
    threads.emplace_back([&]() {
      for (int i = 0; i < 1000; i++) cpq.push(i);
    });

  for (auto &thread : threads) thread.join();
  EXPECT_EQ(cpq.size(), 50000);
  EXPECT_EQ(CountIterations(), 50000);
}

TEST_F(IntQueue, IteratorBegin) {
  AddElems(5);
  auto it = cpq.begin();
  EXPECT_EQ(*it, 4);
}

TEST_F(IntQueue, IteratorPrefixIncrement) {
  AddElems(3);
  auto it = cpq.begin();
  EXPECT_EQ(*(++it), 1);
  EXPECT_EQ(*it, 1);
}

TEST_F(IntQueue, IteratorPostfixIncrement) {
  AddElems(3);
  auto it = cpq.begin();
  EXPECT_EQ(*it++, 2);
  EXPECT_EQ(*it, 1);
}

TEST_F(IntQueue, IteratorEquality) {
  AddElems(5);
  auto it = cpq.begin();
  EXPECT_EQ(it, cpq.begin());
  it++;
  EXPECT_NE(it, cpq.begin());
}

TEST_F(IntQueue, IteratorEnd) {
  AddElems(5);
  auto it = cpq.begin();
  EXPECT_NE(it, cpq.end());
  for (int i = 0; i < 5; i++) it++;
  EXPECT_EQ(it, cpq.end());
  EXPECT_DEATH(*it, "Dereferencing");
}

TEST_F(IntQueue, IteratorCopy) {
  AddElems(5);
  auto it = cpq.begin();
  auto it_copy = it;
  EXPECT_EQ(it, it_copy);
  it++;
  EXPECT_NE(it, it_copy);
  EXPECT_EQ(*it, 3);
  EXPECT_EQ(*it_copy, 4);
}

TEST_F(IntQueue, IteratorMove) {
  AddElems(5);
  auto it = cpq.begin();
  it++;
  EXPECT_EQ(*it, 3);
  decltype(it) it_moved = std::move(it);
  EXPECT_EQ(*it_moved++, 3);
  EXPECT_EQ(*it_moved, 2);
}

TEST_F(IntQueue, IteratorDeleteTail) {
  AddElems(13);
  ASSERT_EQ(cpq.size(), 13);
  ASSERT_EQ(CountIterations(), 13);
  auto it = cpq.begin();
  for (int i = 0; i < 5; i++) it++;
  EXPECT_EQ(it.delete_tail(), 8);
  EXPECT_EQ(it, cpq.end());

  ASSERT_EQ(cpq.size(), 5);
  ASSERT_EQ(CountIterations(), 5);
  auto it2 = cpq.begin();
  EXPECT_EQ(it2.delete_tail(), 5);
  EXPECT_EQ(it2, cpq.end());
  EXPECT_EQ(cpq.size(), 0);
  EXPECT_EQ(CountIterations(), 0);
  EXPECT_EQ(cpq.begin(), cpq.end());
}

TEST_F(IntQueue, IteratorDeleteTailConcurrent) {
  // we will be deleting the whole queue (tail-delete on head)
  // while other threads are pushing to the queue.
  // we'll also ensure that head gets updated in the queue,
  // and delete_tail from an iterator that believes it's the head
  size_t kElems = 500;
  size_t kThreads = 100;
  size_t kMillis = 2;
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreads; i++)
    threads.emplace_back([&]() {
      for (size_t i = 0; i < kElems; i++) {
        cpq.push(i);
        std::this_thread::sleep_for(std::chrono::milliseconds(kMillis));
      }
    });

  size_t deletions = 0;
  while (deletions < kThreads * kElems) {
    auto head_it = cpq.begin();
    // sleep till some thread inserts
    std::this_thread::sleep_for(std::chrono::milliseconds(5 * kMillis));
    deletions += head_it.delete_tail();
  }

  // those threads should be done, but join anyway to avoid aborts
  // due to thread object destruction
  for (auto &thread : threads) thread.join();

  EXPECT_EQ(cpq.size(), 0);
  EXPECT_EQ(CountIterations(), 0);
}

TEST(ConcurrentPushQueue, RvalueLvalueElements) {
  ConcurrentPushQueue<std::string> cpq;
  cpq.push(std::string("rvalue"));
  std::string lvalue("lvalue");
  cpq.push(lvalue);
  std::vector<std::string> expected;
  for (auto &elem : cpq) expected.emplace_back(elem);
  EXPECT_THAT(expected, ::testing::ElementsAre("lvalue", "rvalue"));
}

TEST(ConcurrentPushQueue, Emplace) {
  // test with atomic because it's not copy/move constructable
  ConcurrentPushQueue<std::atomic<int>> cpq;
  cpq.push(3);
  EXPECT_EQ(cpq.size(), 1);
  EXPECT_EQ(cpq.begin()->load(), 3);
  cpq.begin().delete_tail();
  EXPECT_EQ(cpq.size(), 0);
}

TEST_F(IntQueue, ConstQueue) {
  AddElems(5);

  auto const_queue_accepting = [](const ConcurrentPushQueue<int> &const_queue) {
    EXPECT_EQ(const_queue.size(), 5);
    int count = 0;
    for ([[gnu::unused]] int x : const_queue) count++;
    EXPECT_EQ(count, 5);
  };

  const_queue_accepting(cpq);
}
