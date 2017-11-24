#include "gtest/gtest.h"

#include <vector>

#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"

TEST(Engine, GcSnapshot) {
  tx::Engine engine;
  ASSERT_EQ(engine.GcSnapshot(), tx::Snapshot({1}));

  std::vector<tx::Transaction *> transactions;
  // create transactions and check the GC snapshot
  for (int i = 0; i < 5; ++i) {
    transactions.push_back(engine.Begin());
    EXPECT_EQ(engine.GcSnapshot(), tx::Snapshot({1}));
  }

  // commit transactions in the middle, expect
  // the GcSnapshot did not change
  transactions[1]->Commit();
  EXPECT_EQ(engine.GcSnapshot(), tx::Snapshot({1}));
  transactions[2]->Commit();
  EXPECT_EQ(engine.GcSnapshot(), tx::Snapshot({1}));

  // have the first three transactions committed
  transactions[0]->Commit();
  EXPECT_EQ(engine.GcSnapshot(), tx::Snapshot({1, 2, 3, 4}));

  // commit all
  transactions[3]->Commit();
  transactions[4]->Commit();
  EXPECT_EQ(engine.GcSnapshot(), tx::Snapshot({6}));
}

TEST(Engine, Advance) {
  tx::Engine engine;

  auto t0 = engine.Begin();
  auto t1 = engine.Begin();
  EXPECT_EQ(t0->cid(), 1);
  engine.Advance(t0->id_);
  EXPECT_EQ(t0->cid(), 2);
  engine.Advance(t0->id_);
  EXPECT_EQ(t0->cid(), 3);
  EXPECT_EQ(t1->cid(), 1);
}
