#include "gtest/gtest.h"

#include <thread>
#include <vector>

#include "data_structures/concurrent/skiplist.hpp"
#include "transactions/engine_single_node.hpp"
#include "transactions/transaction.hpp"

using namespace tx;

TEST(Engine, GcSnapshot) {
  SingleNodeEngine engine;
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
  SingleNodeEngine engine;

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
  SingleNodeEngine engine;
  std::vector<std::thread> threads;
  SkipList<transaction_id_t> tx_ids;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&engine, accessor = tx_ids.access() ]() mutable {
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
  SingleNodeEngine engine;
  auto t0 = engine.Begin();
  auto t1 = engine.Begin();
  EXPECT_EQ(t0, engine.RunningTransaction(t0->id_));
  EXPECT_NE(t1, engine.RunningTransaction(t0->id_));
  EXPECT_EQ(t1, engine.RunningTransaction(t1->id_));
}

TEST(Engine, EnsureTxIdGreater) {
  SingleNodeEngine engine;
  ASSERT_LE(engine.Begin()->id_, 40);
  engine.EnsureNextIdGreater(42);
  EXPECT_EQ(engine.Begin()->id_, 43);
}
