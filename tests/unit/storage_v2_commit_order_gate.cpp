// Copyright 2026 Memgraph Ltd.
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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <latch>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "storage/v2/commit_order_gate.hpp"

using memgraph::storage::CommitOrderGate;
using memgraph::storage::CommitTicket;

namespace {

template <class Pred>
bool WaitFor(Pred pred, std::chrono::milliseconds timeout) {
  auto const deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred()) {
    if (std::chrono::steady_clock::now() > deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return true;
}

}  // namespace

TEST(CommitOrderGate, EntersInTicketOrderWhateverTheArrivalOrder) {
  CommitOrderGate gate;
  constexpr uint64_t kTickets = 64;
  // Issue in mint order (the storage does this under engine_lock_).
  std::vector<std::unique_ptr<CommitTicket>> tickets;
  tickets.reserve(kTickets);
  for (uint64_t t = 1; t <= kTickets; ++t) tickets.push_back(std::make_unique<CommitTicket>(gate, t));
  EXPECT_EQ(gate.Pending(), kTickets);

  // Arrive at the gate in a shuffled order; every entrant records its exit order.
  std::vector<size_t> order(kTickets);
  std::iota(order.begin(), order.end(), 0);
  std::shuffle(order.begin(), order.end(), std::mt19937{42});
  std::mutex exits_mutex;
  std::vector<uint64_t> exits;
  std::latch all_started{kTickets};
  std::vector<std::thread> threads;
  threads.reserve(kTickets);
  for (auto const index : order) {
    threads.emplace_back([&, index] {
      all_started.count_down();
      all_started.wait();
      auto &ticket = *tickets[index];
      ticket.Enter();
      {
        auto guard = std::lock_guard{exits_mutex};
        exits.push_back(ticket.ticket());
      }
      ticket.MarkPublished();
      ticket.Retire();
    });
  }
  for (auto &thread : threads) thread.join();
  ASSERT_EQ(exits.size(), kTickets);
  EXPECT_TRUE(std::ranges::is_sorted(exits));
  EXPECT_EQ(gate.Pending(), 0);
}

TEST(CommitOrderGate, LaterTicketWaitsForEarlierIssuedEarlier) {
  CommitOrderGate gate;
  CommitTicket first{gate, 10};
  CommitTicket second{gate, 11};
  std::atomic<bool> second_entered{false};
  std::thread waiter{[&] {
    second.Enter();
    second_entered = true;
  }};
  EXPECT_FALSE(WaitFor([&] { return second_entered.load(); }, std::chrono::milliseconds(200)));
  first.Enter();
  EXPECT_FALSE(second_entered.load());
  first.MarkAborted();
  first.Retire();
  EXPECT_TRUE(WaitFor([&] { return second_entered.load(); }, std::chrono::seconds(5)));
  waiter.join();
  second.MarkPublished();
  second.Retire();
  EXPECT_EQ(gate.Pending(), 0);
}

TEST(CommitOrderGate, WaitIdleBlocksUntilAllRetired) {
  CommitOrderGate gate;
  std::optional<CommitTicket> a;
  std::optional<CommitTicket> b;
  a.emplace(gate, 1);
  b.emplace(gate, 2);
  std::atomic<bool> idle{false};
  std::thread waiter{[&] {
    gate.WaitIdle();
    idle = true;
  }};
  EXPECT_FALSE(WaitFor([&] { return idle.load(); }, std::chrono::milliseconds(200)));
  a->Enter();
  a->MarkPublished();
  a->Retire();
  EXPECT_FALSE(WaitFor([&] { return idle.load(); }, std::chrono::milliseconds(200)));
  b->Enter();
  b->MarkAborted();
  b->Retire();
  EXPECT_TRUE(WaitFor([&] { return idle.load(); }, std::chrono::seconds(5)));
  waiter.join();
  // An idle gate returns immediately.
  gate.WaitIdle();
  EXPECT_EQ(gate.Pending(), 0);
}

namespace {

// The assertion message itself goes to the logger, not to stderr, so the death matcher checks the std::terminate
// call MG_ASSERT ends in rather than the message text.
void RetireWithoutMark() {
  CommitOrderGate gate;
  CommitTicket ticket{gate, 1};
  ticket.Enter();
  ticket.Retire();
}

void RetireTwice() {
  CommitOrderGate gate;
  CommitTicket ticket{gate, 1};
  ticket.Enter();
  ticket.MarkPublished();
  ticket.Retire();
  ticket.Retire();
}

void DestroyRegisteredTicket() {
  CommitOrderGate gate;
  CommitTicket ticket{gate, 1};
}

void DestroyPublishedUnretiredTicket() {
  CommitOrderGate gate;
  CommitTicket ticket{gate, 1};
  ticket.Enter();
  ticket.MarkPublished();
}

}  // namespace

TEST(CommitOrderGate, RetireRequiresTerminalMark) {
  {
    CommitOrderGate gate;
    CommitTicket ticket{gate, 1};
    ticket.Enter();
    EXPECT_TRUE(ticket.entered());
    EXPECT_FALSE(ticket.terminal());
    ticket.MarkAborted();
    EXPECT_TRUE(ticket.terminal());
    ticket.Retire();
    EXPECT_TRUE(ticket.retired());
    EXPECT_EQ(gate.Pending(), 0);
  }
  EXPECT_DEATH(RetireWithoutMark(), "terminate called");
  EXPECT_DEATH(RetireTwice(), "terminate called");
}

TEST(CommitOrderGate, DestructorTerminatesOnUnretiredTicket) {
  EXPECT_DEATH(DestroyRegisteredTicket(), "terminate called");
  EXPECT_DEATH(DestroyPublishedUnretiredTicket(), "terminate called");
}

TEST(CommitOrderGate, RecordStateAndIrreversibleMarks) {
  CommitOrderGate gate;
  CommitTicket ticket{gate, 3};
  EXPECT_EQ(ticket.record_state(), CommitTicket::RecordState::not_started);
  EXPECT_FALSE(ticket.incomplete_record());
  ticket.BeginRecord();
  EXPECT_TRUE(ticket.incomplete_record());
  ticket.EndRecord();
  EXPECT_EQ(ticket.record_state(), CommitTicket::RecordState::complete);
  EXPECT_FALSE(ticket.irreversible());
  ticket.MarkIrreversible();
  EXPECT_TRUE(ticket.irreversible());
  ticket.Enter();
  ticket.MarkPublished();
  ticket.Retire();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  return RUN_ALL_TESTS();
}
