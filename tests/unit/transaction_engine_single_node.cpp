#include "gtest/gtest.h"

#include <optional>
#include <thread>
#include <vector>

#include "data_structures/concurrent/skiplist.hpp"
#include "transactions/single_node/engine.hpp"
#include "transactions/transaction.hpp"

using namespace tx;

TEST(Engine, GcSnapshot) {
  Engine engine;
  ASSERT_EQ(engine.GlobalGcSnapshot(), Snapshot({1}));

  std::vector<Transaction *> transactions;
  // create transactions and check the GC snapshot
  for (int i = 0; i < 5; ++i) {
    transactions.push_back(engine.Begin());
    EXPECT_EQ(engine.GlobalGcSnapshot(), Snapshot({1}));
  }

  // commit transactions in the middle, expect
  // the GcSnapshot did not change
  engine.Commit(*transactions[1]);
  EXPECT_EQ(engine.GlobalGcSnapshot(), Snapshot({1}));
  engine.Commit(*transactions[2]);
  EXPECT_EQ(engine.GlobalGcSnapshot(), Snapshot({1}));

  // have the first three transactions committed
  engine.Commit(*transactions[0]);
  EXPECT_EQ(engine.GlobalGcSnapshot(), Snapshot({1, 2, 3, 4}));

  // commit all
  engine.Commit(*transactions[3]);
  engine.Commit(*transactions[4]);
  EXPECT_EQ(engine.GlobalGcSnapshot(), Snapshot({6}));
}

TEST(Engine, Advance) {
  Engine engine;

  auto t0 = engine.Begin();
  auto t1 = engine.Begin();
  EXPECT_EQ(t0->cid(), 1);
  engine.Advance(t0->id_);
  EXPECT_EQ(t0->cid(), 2);
  engine.Advance(t0->id_);
  EXPECT_EQ(t0->cid(), 3);
  EXPECT_EQ(t1->cid(), 1);
}

TEST(Engine, ConcurrentBegin) {
  Engine engine;
  std::vector<std::thread> threads;
  SkipList<TransactionId> tx_ids;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&engine, accessor = tx_ids.access()]() mutable {
      for (int j = 0; j < 100; ++j) {
        auto t = engine.Begin();
        accessor.insert(t->id_);
      }
    });
  }
  for (auto &t : threads) t.join();
  EXPECT_EQ(tx_ids.access().size(), 1000);
}

TEST(Engine, RunningTransaction) {
  Engine engine;
  auto t0 = engine.Begin();
  auto t1 = engine.Begin();
  EXPECT_EQ(t0, engine.RunningTransaction(t0->id_));
  EXPECT_NE(t1, engine.RunningTransaction(t0->id_));
  EXPECT_EQ(t1, engine.RunningTransaction(t1->id_));
}

TEST(Engine, EnsureTxIdGreater) {
  Engine engine;
  ASSERT_LE(engine.Begin()->id_, 40);
  engine.EnsureNextIdGreater(42);
  EXPECT_EQ(engine.Begin()->id_, 43);
}

TEST(Engine, BlockingTransaction) {
  Engine engine;
  std::vector<std::thread> threads;
  std::atomic<bool> finished{false};
  std::atomic<bool> blocking_started{false};
  std::atomic<bool> blocking_finished{false};
  std::atomic<int> tx_counter{0};
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&engine, &tx_counter, &finished]() mutable {
      auto t = engine.Begin();
      tx_counter++;
      while (!finished.load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      engine.Commit(*t);
    });
  }

  // Wait for all transactions to start.
  do {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  } while (tx_counter.load() < 10);

  threads.emplace_back([&engine, &blocking_started, &blocking_finished]() {
    // This should block until other transactions end.
    blocking_started.store(true);
    auto t = engine.BeginBlocking(std::nullopt);
    engine.Commit(*t);
    blocking_finished.store(true);
  });

  EXPECT_FALSE(finished.load());
  EXPECT_FALSE(blocking_finished.load());
  EXPECT_EQ(tx_counter.load(), 10);

  // Make sure the blocking transaction thread kicked off.
  do {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  } while (!blocking_started.load());

  // Make sure we can't start any new transaction
  EXPECT_THROW(engine.Begin(), TransactionEngineError);
  EXPECT_THROW(engine.BeginBlocking(std::nullopt), TransactionEngineError);

  // Release regular transactions. This will cause the blocking transaction to
  // end also.
  finished.store(true);

  for (auto &t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  EXPECT_TRUE(blocking_finished.load());

  // Make sure we can start transactions now.
  {
    auto t = engine.Begin();
    EXPECT_NE(t, nullptr);
    engine.Commit(*t);
  }
  {
    auto t = engine.BeginBlocking(std::nullopt);
    EXPECT_NE(t, nullptr);
    engine.Commit(*t);
  }
}
