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

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "communication/v2/epoll_poller.hpp"

using memgraph::communication::v2::EpollPoller;
using memgraph::communication::v2::PollTarget;

namespace {

struct Target final : PollTarget {
  std::atomic<int> dispatched{0};
  std::atomic<int> ran{0};

  void Dispatch() override { dispatched.fetch_add(1); }

  void RunInline(memgraph::utils::Priority /*unused*/) override { ran.fetch_add(1); }
};

struct Pair {
  int server{-1};  // adopted by the poller
  int client{-1};

  Pair() {
    int fds[2];
    EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds), 0);
    server = fds[0];
    client = fds[1];
  }

  ~Pair() {
    if (client >= 0) ::close(client);
  }

  Pair(const Pair &) = delete;
  Pair &operator=(const Pair &) = delete;

  void Send() const { ASSERT_EQ(::write(client, "x", 1), 1); }

  void Drain() const {
    char c[16];
    while (::read(server, c, sizeof(c)) > 0) {
    }
  }
};

size_t Poll(EpollPoller &poller, const int timeout_ms, std::vector<std::shared_ptr<PollTarget>> &claimed) {
  std::array<std::shared_ptr<PollTarget>, EpollPoller::kMaxEventsPerPoll> out;
  const auto n = poller.PollOnce(timeout_ms, false, out);
  for (size_t i = 0; i < n; ++i) claimed.push_back(std::move(out[i]));
  return n;
}

}  // namespace

TEST(EpollPoller, ClaimExactlyOnceAcrossConcurrentPollers) {
  EpollPoller poller;
  Pair pair;
  auto target = std::make_shared<Target>();
  const auto slot = poller.Adopt(pair.server, target);
  ASSERT_NE(slot, EpollPoller::kInvalid);

  constexpr int kRounds = 150;
  constexpr int kPollers = 4;
  for (int round = 0; round < kRounds; ++round) {
    ASSERT_TRUE(poller.Arm(slot));
    pair.Send();
    std::atomic<int> claims{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kPollers; ++t) {
      threads.emplace_back([&] {
        std::vector<std::shared_ptr<PollTarget>> got;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(20);
        while (std::chrono::steady_clock::now() < deadline) {
          Poll(poller, 0, got);
        }
        claims.fetch_add(static_cast<int>(got.size()));
      });
    }
    for (auto &t : threads) t.join();
    ASSERT_EQ(claims.load(), 1) << "round " << round;
    pair.Drain();  // owner consumed the data; the next Arm() starts from a quiet fd
  }
  poller.Close(slot);
}

TEST(EpollPoller, EventWhileRunningIsReevaluatedByArm) {
  EpollPoller poller;
  Pair pair;
  auto target = std::make_shared<Target>();
  const auto slot = poller.Adopt(pair.server, target);
  ASSERT_NE(slot, EpollPoller::kInvalid);

  pair.Send();  // arrives while RUNNING (disarmed)
  std::vector<std::shared_ptr<PollTarget>> got;
  EXPECT_EQ(Poll(poller, 20, got), 0U);

  ASSERT_TRUE(poller.Arm(slot));
  EXPECT_EQ(Poll(poller, 1000, got), 1U);  // the arm itself sees the pending data

  // Data still unread: re-arming fires again.
  ASSERT_TRUE(poller.Arm(slot));
  EXPECT_EQ(Poll(poller, 1000, got), 1U);

  // Nothing to claim while RUNNING.
  EXPECT_EQ(Poll(poller, 20, got), 0U);
  poller.Close(slot);
}

TEST(EpollPoller, StaleSlotHandleIsRejectedAfterReuse) {
  EpollPoller poller;
  Pair first;
  auto t1 = std::make_shared<Target>();
  const auto old_slot = poller.Adopt(first.server, t1);
  ASSERT_NE(old_slot, EpollPoller::kInvalid);
  ASSERT_TRUE(poller.Arm(old_slot));
  ASSERT_TRUE(poller.TryBeginClose(old_slot));
  auto keep = poller.Close(old_slot);
  EXPECT_EQ(keep, t1);

  Pair second;
  auto t2 = std::make_shared<Target>();
  const auto new_slot = poller.Adopt(second.server, t2);
  ASSERT_NE(new_slot, EpollPoller::kInvalid);
  EXPECT_NE(new_slot, old_slot);
  ASSERT_TRUE(poller.Arm(new_slot));

  // The old handle must not be able to close the recycled slot.
  EXPECT_FALSE(poller.TryBeginClose(old_slot));

  second.Send();
  std::vector<std::shared_ptr<PollTarget>> got;
  ASSERT_EQ(Poll(poller, 1000, got), 1U);
  EXPECT_EQ(got[0], t2);
  poller.Close(new_slot);
}

TEST(EpollPoller, CloseRacingAnEventHasExactlyOneWinner) {
  EpollPoller poller;
  for (int round = 0; round < 500; ++round) {
    Pair pair;
    auto target = std::make_shared<Target>();
    const auto slot = poller.Adopt(pair.server, target);
    ASSERT_NE(slot, EpollPoller::kInvalid);
    ASSERT_TRUE(poller.Arm(slot));

    std::atomic<bool> go{false};
    std::vector<std::shared_ptr<PollTarget>> got;
    bool closed = false;
    std::thread poll_thread([&] {
      while (!go.load()) {
      }
      pair.Send();
      Poll(poller, 20, got);
    });
    std::thread close_thread([&] {
      while (!go.load()) {
      }
      closed = poller.TryBeginClose(slot);
    });
    go.store(true);
    poll_thread.join();
    close_thread.join();

    ASSERT_EQ(static_cast<int>(got.size()) + (closed ? 1 : 0), 1) << "round " << round;
    poller.Close(slot);  // the winner owns the slot either way
  }
}

TEST(EpollPoller, TryClaimReturnsFirstAndDispatchesTheRest) {
  EpollPoller poller;
  std::vector<std::unique_ptr<Pair>> pairs;
  std::vector<std::shared_ptr<Target>> targets;
  std::vector<EpollPoller::Slot> slots;
  constexpr int kSessions = 3;
  for (int i = 0; i < kSessions; ++i) {
    pairs.push_back(std::make_unique<Pair>());
    targets.push_back(std::make_shared<Target>());
    slots.push_back(poller.Adopt(pairs.back()->server, targets.back()));
    ASSERT_TRUE(poller.Arm(slots.back()));
  }
  EXPECT_EQ(poller.TryClaim(), nullptr);  // nothing ready

  for (auto &p : pairs) p->Send();
  auto first = poller.TryClaim();
  ASSERT_NE(first, nullptr);
  int dispatched = 0;
  for (auto &t : targets) dispatched += t->dispatched.load();
  EXPECT_EQ(dispatched, kSessions - 1);

  poller.CloseAll();
}

TEST(EpollPoller, FallbackThreadDispatchesOnceAndStopsPromptly) {
  EpollPoller poller;
  poller.Start();
  Pair pair;
  auto target = std::make_shared<Target>();
  const auto slot = poller.Adopt(pair.server, target);
  ASSERT_NE(slot, EpollPoller::kInvalid);
  ASSERT_TRUE(poller.Arm(slot));
  pair.Send();

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (target->dispatched.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(target->dispatched.load(), 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_EQ(target->dispatched.load(), 1);  // ONESHOT: no second delivery without a re-arm

  const auto start = std::chrono::steady_clock::now();
  poller.Stop();
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
  poller.Stop();  // idempotent

  // Claimed by the fallback thread, still RUNNING: CloseAll reclaims it and drops the keep-alive.
  poller.CloseAll();
  EXPECT_EQ(target.use_count(), 1);
}

TEST(EpollPoller, CloseAllClosesArmedAndRunningSlots) {
  EpollPoller poller;
  Pair armed;
  Pair running;
  auto t1 = std::make_shared<Target>();
  auto t2 = std::make_shared<Target>();
  const auto s1 = poller.Adopt(armed.server, t1);
  const auto s2 = poller.Adopt(running.server, t2);
  ASSERT_NE(s1, EpollPoller::kInvalid);
  ASSERT_NE(s2, EpollPoller::kInvalid);
  ASSERT_TRUE(poller.Arm(s1));

  poller.CloseAll();
  EXPECT_EQ(t1.use_count(), 1);
  EXPECT_EQ(t2.use_count(), 1);
  char c;
  EXPECT_EQ(::read(armed.client, &c, 1), 0);  // peer sees EOF: the adopted fd was closed
  EXPECT_EQ(::read(running.client, &c, 1), 0);
}
