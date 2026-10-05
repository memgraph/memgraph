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

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "communication/v2/epoll_poller.hpp"

using memgraph::communication::v2::EpollPoller;
using memgraph::communication::v2::PollTarget;

namespace memgraph::communication::v2 {
struct EpollPollerTestAccess {
  static std::shared_ptr<PollTarget> ClaimEvent(EpollPoller &poller, uint64_t tag) { return poller.ClaimEvent(tag); }
};
}  // namespace memgraph::communication::v2

using memgraph::communication::v2::EpollPollerTestAccess;

namespace {

struct Target final : PollTarget {
  std::atomic<int> dispatched{0};
  std::atomic<int> ran{0};
  std::atomic<int> force_closed{0};

  void OnForceClosed() override { force_closed.fetch_add(1); }

  void Dispatch() override { dispatched.fetch_add(1); }

  void RunInline(memgraph::utils::Priority /*unused*/) override { ran.fetch_add(1); }
};

struct Pair {
  int server{-1};
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
  auto target = poller.PollOne(timeout_ms, false);
  if (!target) return 0;
  claimed.push_back(std::move(target));
  return 1;
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

  pair.Send();
  std::vector<std::shared_ptr<PollTarget>> got;
  EXPECT_EQ(Poll(poller, 20, got), 0U);

  ASSERT_TRUE(poller.Arm(slot));
  EXPECT_EQ(Poll(poller, 1000, got), 1U);

  // Level-triggered: data still unread, so re-arming fires again.
  ASSERT_TRUE(poller.Arm(slot));
  EXPECT_EQ(Poll(poller, 1000, got), 1U);

  EXPECT_EQ(Poll(poller, 20, got), 0U);
  poller.Close(slot);
}

TEST(EpollPoller, StaleHandleIsRejectedAfterSlotReuse) {
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
  ASSERT_EQ(new_slot >> 32, old_slot >> 32);
  ASSERT_TRUE(poller.Arm(new_slot));

  // The old handle must neither close nor claim the recycled, ARMED slot.
  EXPECT_FALSE(poller.TryBeginClose(old_slot));
  EXPECT_EQ(EpollPollerTestAccess::ClaimEvent(poller, old_slot), nullptr);

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

TEST(EpollPoller, ClaimAndCloseAreMutuallyExclusiveInBothOrders) {
  EpollPoller poller;

  {  // claimed first: the slot is RUNNING, so it can be neither closed from outside nor claimed again
    Pair pair;
    auto target = std::make_shared<Target>();
    const auto slot = poller.Adopt(pair.server, target);
    ASSERT_NE(slot, EpollPoller::kInvalid);
    ASSERT_TRUE(poller.Arm(slot));
    pair.Send();
    auto claimed = poller.PollOne(1000, false);
    ASSERT_EQ(claimed, target);
    EXPECT_FALSE(poller.TryBeginClose(slot));
    EXPECT_EQ(EpollPollerTestAccess::ClaimEvent(poller, slot), nullptr);
    poller.Close(slot);
  }

  {  // close first: the event that follows must not be claimable, and the closer still owns the slot
    Pair pair;
    auto target = std::make_shared<Target>();
    const auto slot = poller.Adopt(pair.server, target);
    ASSERT_NE(slot, EpollPoller::kInvalid);
    ASSERT_TRUE(poller.Arm(slot));
    ASSERT_TRUE(poller.TryBeginClose(slot));
    pair.Send();
    EXPECT_EQ(poller.PollOne(100, false), nullptr);
    EXPECT_FALSE(poller.TryBeginClose(slot));
    EXPECT_EQ(poller.Close(slot), target);
  }
}

TEST(EpollPoller, TryClaimClaimsOneReadySessionPerCallExactlyOnce) {
  EpollPoller poller;
  std::vector<std::unique_ptr<Pair>> pairs;
  std::vector<std::shared_ptr<Target>> targets;
  constexpr int kSessions = 3;
  for (int i = 0; i < kSessions; ++i) {
    pairs.push_back(std::make_unique<Pair>());
    targets.push_back(std::make_shared<Target>());
    ASSERT_TRUE(poller.Arm(poller.Adopt(pairs.back()->server, targets.back())));
  }
  EXPECT_EQ(poller.TryClaim(), nullptr);

  for (auto &p : pairs) p->Send();
  // TryClaim hands the session to the caller; it must never Dispatch behind the caller's back.
  std::vector<std::shared_ptr<memgraph::utils::IdleRunnable>> claimed;
  for (int i = 0; i < kSessions; ++i) {
    auto one = poller.TryClaim();
    ASSERT_NE(one, nullptr) << "call " << i;
    claimed.push_back(std::move(one));
  }
  EXPECT_EQ(poller.TryClaim(), nullptr);
  for (auto &t : targets) EXPECT_EQ(t->dispatched.load(), 0);
  for (size_t i = 0; i < claimed.size(); ++i) {
    for (size_t j = i + 1; j < claimed.size(); ++j) EXPECT_NE(claimed[i], claimed[j]);
  }

  claimed.clear();
  poller.CloseAll();
}

TEST(EpollPoller, ReadableWakeFdDoesNotHideReadySessionFromTryClaim) {
  EpollPoller poller;
  Pair pair;
  auto target = std::make_shared<Target>();
  const auto slot = poller.Adopt(pair.server, target);
  ASSERT_TRUE(poller.Arm(slot));
  poller.Wake();
  pair.Send();

  auto claimed = poller.TryClaim();
  if (!claimed) claimed = poller.TryClaim();  // round-robin: the undrained wake fd may take the slot first
  ASSERT_NE(claimed, nullptr);
  EXPECT_EQ(claimed.get(), static_cast<memgraph::utils::IdleRunnable *>(target.get()));
  claimed.reset();
  EXPECT_EQ(poller.TryClaim(), nullptr);
  // TryClaim never drains the wake fd: the blocking poller must still see it and return at once, not at the timeout.
  const auto start = std::chrono::steady_clock::now();
  poller.WaitAndDispatch(std::chrono::seconds(10));
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
  poller.CloseAll();
}

TEST(EpollPoller, WaitAndDispatchReturnsAtDeadlineWithNothingReady) {
  EpollPoller poller;
  const auto start = std::chrono::steady_clock::now();
  poller.WaitAndDispatch(std::chrono::milliseconds(30));
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_GE(elapsed, std::chrono::milliseconds(25));
  EXPECT_LT(elapsed, std::chrono::seconds(5));
}

TEST(EpollPoller, WaitAndDispatchReturnsEarlyOnWake) {
  EpollPoller poller;
  std::atomic<bool> returned{false};
  std::thread waiter([&] {
    poller.WaitAndDispatch(std::chrono::seconds(30));
    returned.store(true);
  });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!returned.load() && std::chrono::steady_clock::now() < deadline) {
    poller.Wake();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(returned.load());
  waiter.join();
}

TEST(EpollPoller, WaitAndDispatchDispatchesEveryReadyTargetExactlyOnce) {
  EpollPoller poller;
  constexpr int kSessions = 5;
  std::vector<std::unique_ptr<Pair>> pairs;
  std::vector<std::shared_ptr<Target>> targets;
  for (int i = 0; i < kSessions; ++i) {
    pairs.push_back(std::make_unique<Pair>());
    targets.push_back(std::make_shared<Target>());
    ASSERT_TRUE(poller.Arm(poller.Adopt(pairs.back()->server, targets.back())));
    pairs.back()->Send();
  }
  for (int i = 0; i < kSessions; ++i) {  // one target per wake
    poller.WaitAndDispatch(std::chrono::seconds(5));
    int total = 0;
    for (auto &t : targets) total += t->dispatched.load();
    ASSERT_EQ(total, i + 1) << "call " << i;
  }
  poller.WaitAndDispatch(std::chrono::milliseconds(10));  // ONESHOT: nothing more without a re-arm
  for (auto &t : targets) {
    EXPECT_EQ(t->dispatched.load(), 1);
    EXPECT_EQ(t->ran.load(), 0);
  }
  poller.CloseAll();
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

  const int armed_fd = armed.server;
  const int running_fd = running.server;
  poller.CloseAll();
  for (const int fd : {armed_fd, running_fd}) {
    errno = 0;
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1) << "fd " << fd << " still open";
    EXPECT_EQ(errno, EBADF);
  }
  EXPECT_EQ(t1.use_count(), 1);
  EXPECT_EQ(t2.use_count(), 1);
  EXPECT_EQ(t1->force_closed.load(), 1);
  EXPECT_EQ(t2->force_closed.load(), 1);
  char c;
  EXPECT_EQ(::read(armed.client, &c, 1), 0);
  EXPECT_EQ(::read(running.client, &c, 1), 0);
}

TEST(EpollPoller, CloseAllRacingTryBeginCloseClosesExactlyOnce) {
  EpollPoller poller;
  constexpr int kRounds = 2000;
  int terminator_wins = 0;
  for (int round = 0; round < kRounds; ++round) {
    Pair pair;
    auto target = std::make_shared<Target>();
    const auto slot = poller.Adopt(pair.server, target);
    ASSERT_NE(slot, EpollPoller::kInvalid);
    ASSERT_TRUE(poller.Arm(slot));

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<int> closes{0};
    auto sync = [&] {
      ready.fetch_add(1);
      while (!go.load()) {
      }
    };
    std::thread terminator([&] {
      sync();
      if (poller.TryBeginClose(slot)) {
        auto keep = poller.Close(slot);
        EXPECT_NE(keep, nullptr);
        closes.fetch_add(1);
      }
    });
    std::thread closer([&] {
      sync();
      poller.CloseAll();
    });
    while (ready.load() < 2) {
    }
    go.store(true);
    terminator.join();
    closer.join();

    ASSERT_EQ(target->force_closed.load() + closes.load(), 1) << "round " << round;
    terminator_wins += closes.load();

    char c;
    ASSERT_EQ(::read(pair.client, &c, 1), 0) << "peer must see EOF from the single close, round " << round;
    ASSERT_EQ(target.use_count(), 1) << "keep-alive reference leaked or double-dropped, round " << round;

    // A duplicate free-list entry would hand the same index out twice.
    Pair a;
    Pair b;
    const auto sa = poller.Adopt(a.server, std::make_shared<Target>());
    const auto sb = poller.Adopt(b.server, std::make_shared<Target>());
    ASSERT_NE(sa, EpollPoller::kInvalid);
    ASSERT_NE(sb, EpollPoller::kInvalid);
    ASSERT_NE(static_cast<uint32_t>(sa >> 32), static_cast<uint32_t>(sb >> 32)) << "round " << round;
    poller.Close(sa);
    poller.Close(sb);
  }
  RecordProperty("terminator_wins", terminator_wins);
}
