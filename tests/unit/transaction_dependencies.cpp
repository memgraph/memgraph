// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <chrono>
#include <latch>
#include <thread>

#include "storage/v2/transaction_dependencies.hpp"

namespace ms = memgraph::storage;

TEST(TransactionDependenciesTest, OneTransactionNeverWaits) {
  ms::TransactionDependencies tx_deps;
  tx_deps.RegisterTransaction(1);

  bool result = tx_deps.WaitFor(1, 999);

  EXPECT_TRUE(result);
}

TEST(TransactionDependenciesTest, DisjointTransactionsNeverWait) {
  ms::TransactionDependencies tx_deps;
  tx_deps.RegisterTransaction(1);
  tx_deps.RegisterTransaction(2);
  tx_deps.RegisterTransaction(3);

  bool result1 = tx_deps.WaitFor(1, 4);
  bool result2 = tx_deps.WaitFor(2, 5);
  bool result3 = tx_deps.WaitFor(3, 6);

  EXPECT_TRUE(result1);
  EXPECT_TRUE(result2);
  EXPECT_TRUE(result3);
}

TEST(TransactionDependenciesTest, DependentTransactionMustWait) {
  ms::TransactionDependencies tx_deps;
  tx_deps.RegisterTransaction(1);
  tx_deps.RegisterTransaction(2);

  std::latch waiter_started{1};

  std::jthread waiter([&]() {
    waiter_started.count_down();
    bool result = tx_deps.WaitFor(1, 2);
    EXPECT_TRUE(result);
  });

  waiter_started.wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  tx_deps.UnregisterTransaction(2);
}

TEST(TransactionDependenciesTest, TransitivelyDependentTransactionsMustWait) {
  ms::TransactionDependencies tx_deps;
  tx_deps.RegisterTransaction(1);
  tx_deps.RegisterTransaction(2);
  tx_deps.RegisterTransaction(3);

  std::latch both_started{2};

  std::jthread t1_waiter([&]() {
    both_started.count_down();
    bool result = tx_deps.WaitFor(1, 2);
    EXPECT_TRUE(result);
  });

  std::jthread t2_waiter([&]() {
    both_started.count_down();
    bool result = tx_deps.WaitFor(2, 3);
    EXPECT_TRUE(result);
  });

  both_started.wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  tx_deps.UnregisterTransaction(3);
  tx_deps.UnregisterTransaction(2);
}

TEST(TransactionDependenciesTest, MultipleTransactionsCanWaitOnOne) {
  ms::TransactionDependencies tx_deps;
  tx_deps.RegisterTransaction(1);
  tx_deps.RegisterTransaction(2);
  tx_deps.RegisterTransaction(3);

  std::latch both_started{2};

  std::jthread t1_waiter([&]() {
    both_started.count_down();
    bool result = tx_deps.WaitFor(1, 3);
    EXPECT_TRUE(result);
  });

  std::jthread t2_waiter([&]() {
    both_started.count_down();
    bool result = tx_deps.WaitFor(2, 3);
    EXPECT_TRUE(result);
  });

  both_started.wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  tx_deps.UnregisterTransaction(3);
}

TEST(TransactionDependenciesTest, CircularDependenciesAreUnresolvable) {
  ms::TransactionDependencies tx_deps;
  tx_deps.RegisterTransaction(1);
  tx_deps.RegisterTransaction(2);

  std::latch both_started{2};
  std::atomic<int> num_unresolvable_conflicts{0};

  std::jthread t1([&]() {
    both_started.count_down();
    bool result = tx_deps.WaitFor(1, 2);
    if (!result) num_unresolvable_conflicts++;
  });

  std::jthread t2([&]() {
    both_started.count_down();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    bool result = tx_deps.WaitFor(2, 1);
    if (!result) num_unresolvable_conflicts++;
  });

  both_started.wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  EXPECT_GE(num_unresolvable_conflicts.load(), 1);

  tx_deps.UnregisterTransaction(1);
  tx_deps.UnregisterTransaction(2);
}

TEST(TransactionDependenciesTest, TransitiveCircularDependenciesAreUnresolvable) {
  ms::TransactionDependencies tx_deps;
  tx_deps.RegisterTransaction(1);
  tx_deps.RegisterTransaction(2);
  tx_deps.RegisterTransaction(3);

  std::latch all_started{3};
  std::atomic<int> num_unresolvable_conflicts{0};

  std::jthread t1([&]() {
    all_started.count_down();
    bool result = tx_deps.WaitFor(1, 2);
    if (!result) num_unresolvable_conflicts++;
  });

  std::jthread t2([&]() {
    all_started.count_down();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    bool result = tx_deps.WaitFor(2, 3);
    if (!result) num_unresolvable_conflicts++;
  });

  std::jthread t3([&]() {
    all_started.count_down();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    bool result = tx_deps.WaitFor(3, 1);
    if (!result) num_unresolvable_conflicts++;
  });

  all_started.wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  EXPECT_EQ(num_unresolvable_conflicts.load(), 1);

  tx_deps.UnregisterTransaction(1);
  tx_deps.UnregisterTransaction(2);
  tx_deps.UnregisterTransaction(3);
}
