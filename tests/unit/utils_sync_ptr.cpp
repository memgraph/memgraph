// Copyright 2023 Memgraph Ltd.
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

#include <atomic>
#include <chrono>
#include <thread>

#include <utils/sync_ptr.hpp>
#include "utils/exceptions.hpp"

using namespace std::chrono_literals;

TEST(SyncPtr, Basic) {
  std::atomic_bool alive{false};
  struct Test {
    Test(std::atomic_bool &alive) : alive_(alive) { alive_ = true; }
    ~Test() { alive_ = false; }
    std::atomic_bool &alive_;
  };

  ASSERT_FALSE(alive);

  memgraph::utils::SyncPtr<Test> sp(alive);
  ASSERT_TRUE(alive);
  auto sp_copy1 = sp.get();
  auto sp_copy2 = sp.get();

  sp_copy1.reset();
  ASSERT_TRUE(alive);
  sp_copy2.reset();
  ASSERT_TRUE(alive);

  sp.DestroyAndSync();
  ASSERT_FALSE(alive);
}

TEST(SyncPtr, BasicWConfig) {
  std::atomic_bool alive{false};
  struct Test {
    Test(std::atomic_bool &alive) : alive_(alive) { alive_ = true; }
    ~Test() { alive_ = false; }
    std::atomic_bool &alive_;
  };

  struct TestConf {
    TestConf(int i) : conf_(i) {}
    int conf_;
  };

  ASSERT_FALSE(alive);

  memgraph::utils::SyncPtr<Test, TestConf> sp(123, alive);
  ASSERT_TRUE(alive);
  ASSERT_EQ(sp.config().conf_, 123);
  auto sp_copy1 = sp.get();
  auto sp_copy2 = sp.get();

  sp_copy1.reset();
  ASSERT_TRUE(alive);
  sp_copy2.reset();
  ASSERT_TRUE(alive);

  sp.DestroyAndSync();
  ASSERT_FALSE(alive);
}

TEST(SyncPtr, Sync) {
  std::atomic_bool alive{false};
  struct Test {
    Test(std::atomic_bool &alive) : alive_(alive) { alive_ = true; }
    ~Test() { alive_ = false; }
    std::atomic_bool &alive_;
  };

  std::thread th;

  ASSERT_FALSE(alive);

  memgraph::utils::SyncPtr<Test> sp(alive);

  {
    using namespace std::chrono_literals;
    sp.timeout(10000ms);  // 10sec

    ASSERT_TRUE(alive);
    auto sp_copy1 = sp.get();
    auto sp_copy2 = sp.get();

    th = std::thread([&alive, p = sp.get()]() mutable {
      // Wait for a second and then release the pointer
      // SyncPtr will be destroyed in the mean time (and block)
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      ASSERT_TRUE(alive);
      p.reset();
      ASSERT_FALSE(alive);
    });
  }

  ASSERT_TRUE(alive);
  sp.DestroyAndSync();

  th.join();
  ASSERT_FALSE(alive);
}

TEST(SyncPtr, SyncWConfig) {
  std::atomic_bool alive{false};
  struct Test {
    Test(std::atomic_bool &alive) : alive_(alive) { alive_ = true; }
    ~Test() { alive_ = false; }
    std::atomic_bool &alive_;
  };

  struct TestConf {
    TestConf(int i) : conf_(i) {}
    int conf_;
  };

  std::thread th;

  ASSERT_FALSE(alive);

  memgraph::utils::SyncPtr<Test, TestConf> sp(456, alive);

  {
    using namespace std::chrono_literals;
    sp.timeout(10000ms);  // 10sec
    ASSERT_TRUE(alive);
    ASSERT_EQ(sp.config().conf_, 456);
    auto sp_copy1 = sp.get();
    auto sp_copy2 = sp.get();

    th = std::thread([&alive, p = sp.get()]() mutable {
      // Wait for a second and then release the pointer
      // SyncPtr will be destroyed in the mean time (and block)
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      ASSERT_TRUE(alive);
      p.reset();
      ASSERT_FALSE(alive);
    });
  }

  ASSERT_TRUE(alive);
  sp.DestroyAndSync();

  th.join();
  ASSERT_FALSE(alive);
}

TEST(SyncPtr, Timeout) {
  std::atomic_bool alive{false};
  struct Test {
    Test(std::atomic_bool &alive) : alive_(alive) { alive_ = true; }
    ~Test() { alive_ = false; }
    std::atomic_bool &alive_;
  };

  std::thread th;

  ASSERT_FALSE(alive);

  memgraph::utils::SyncPtr<Test> sp(alive);
  using namespace std::chrono_literals;
  sp.timeout(100ms);  // 10sec

  ASSERT_TRUE(alive);

  auto p = sp.get();

  ASSERT_TRUE(alive);
  ASSERT_THROW(sp.DestroyAndSync(), memgraph::utils::BasicException);

  p.reset();
  ASSERT_FALSE(alive);
}
