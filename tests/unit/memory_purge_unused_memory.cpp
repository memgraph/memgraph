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

#include "memory/global_memory_control.hpp"

#if USE_JEMALLOC

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <jemalloc/jemalloc.h>
#include <sys/types.h>

namespace {

using namespace std::chrono_literals;

template <typename T>
  requires std::is_trivially_copyable_v<T>
T ReadMallctl(const std::string &name) {
  T value{};
  size_t size = sizeof(T);
  EXPECT_EQ(je_mallctl(name.c_str(), &value, &size, nullptr, 0), 0) << name;
  return value;
}

template <typename T>
  requires std::is_trivially_copyable_v<T>
void WriteMallctl(const std::string &name, T value) {
  ASSERT_EQ(je_mallctl(name.c_str(), nullptr, nullptr, &value, sizeof(T)), 0) << name;
}

size_t UnpurgedPages(unsigned arena) {
  uint64_t epoch = 1;
  size_t size = sizeof(epoch);
  EXPECT_EQ(je_mallctl("epoch", &epoch, &size, &epoch, size), 0);
  const auto prefix = "stats.arenas." + std::to_string(arena);
  return ReadMallctl<size_t>(prefix + ".pdirty") + ReadMallctl<size_t>(prefix + ".pmuzzy");
}

// An arena whose extent hooks hold any thread but the owning test thread inside the first hook it
// enters until Release(). The jemalloc background thread purges by calling these hooks, so it can
// be held in the middle of a decay pass, with that arena marked as being purged. The hooks find the
// arena through a static pointer, so only one may exist at a time.
class HeldPurgeArena {
 public:
  HeldPurgeArena() = default;

  HeldPurgeArena(const HeldPurgeArena &) = delete;
  HeldPurgeArena &operator=(const HeldPurgeArena &) = delete;
  HeldPurgeArena(HeldPurgeArena &&) = delete;
  HeldPurgeArena &operator=(HeldPurgeArena &&) = delete;

  // jemalloc keeps a pointer to hooks_, so the arena must be handed back its own hooks first.
  ~HeldPurgeArena() {
    Release();
    if (hooks_installed_) {
      EXPECT_EQ(je_mallctl(Name("extent_hooks").c_str(), nullptr, nullptr, &base_, sizeof(base_)), 0);
    }
    if (created_) EXPECT_EQ(je_mallctl(Name("destroy").c_str(), nullptr, nullptr, nullptr, 0), 0);
    instance_ = nullptr;
  }

  // Call through ASSERT_NO_FATAL_FAILURE: a test on an arena without these hooks would test nothing.
  void Install() {
    ASSERT_EQ(instance_, nullptr);
    size_t size = sizeof(arena_);
    ASSERT_EQ(je_mallctl("arenas.create", &arena_, &size, nullptr, 0), 0);
    created_ = true;
    size = sizeof(base_);
    ASSERT_EQ(je_mallctl(Name("extent_hooks").c_str(), &base_, &size, nullptr, 0), 0);
    hooks_ = *base_;
    hooks_.dalloc = &Dalloc;
    hooks_.purge_lazy = &PurgeLazy;
    hooks_.purge_forced = &PurgeForced;
    instance_ = this;
    extent_hooks_t *mine = &hooks_;
    ASSERT_EQ(je_mallctl(Name("extent_hooks").c_str(), nullptr, nullptr, &mine, sizeof(mine)), 0);
    hooks_installed_ = true;
  }

  unsigned Index() const { return arena_; }

  int Flags() const { return MALLOCX_ARENA(arena_) | MALLOCX_TCACHE_NONE; }

  bool WaitUntilHeld(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    return cv_.wait_for(lock, timeout, [this] { return held_; });
  }

  void Release() {
    {
      const std::lock_guard lock(mutex_);
      released_ = true;
    }
    cv_.notify_all();
  }

 private:
  std::string Name(const char *leaf) const { return "arena." + std::to_string(arena_) + "." + leaf; }

  void MaybeHold() {
    if (std::this_thread::get_id() == owner_) return;
    std::unique_lock lock(mutex_);
    held_ = true;
    cv_.notify_all();
    cv_.wait(lock, [this] { return released_; });
  }

  static bool Dalloc(extent_hooks_t * /*hooks*/, void *addr, size_t size, bool committed, unsigned arena_ind) {
    instance_->MaybeHold();
    return instance_->base_->dalloc(instance_->base_, addr, size, committed, arena_ind);
  }

  static bool PurgeLazy(extent_hooks_t * /*hooks*/, void *addr, size_t size, size_t offset, size_t length,
                        unsigned arena_ind) {
    instance_->MaybeHold();
    return instance_->base_->purge_lazy(instance_->base_, addr, size, offset, length, arena_ind);
  }

  static bool PurgeForced(extent_hooks_t * /*hooks*/, void *addr, size_t size, size_t offset, size_t length,
                          unsigned arena_ind) {
    instance_->MaybeHold();
    return instance_->base_->purge_forced(instance_->base_, addr, size, offset, length, arena_ind);
  }

  static inline HeldPurgeArena *instance_ = nullptr;

  unsigned arena_{0};
  extent_hooks_t *base_{nullptr};
  extent_hooks_t hooks_{};
  std::thread::id owner_{std::this_thread::get_id()};
  std::mutex mutex_;
  std::condition_variable cv_;
  bool created_{false};
  bool hooks_installed_{false};
  bool held_{false};
  bool released_{false};
};

constexpr size_t kObjectBytes = 64 * 1024;
constexpr int kObjects = 256;

// Background threads are process-wide, so every test starts with them running and leaves them
// running however it ends.
class PurgeUnusedMemoryTest : public ::testing::Test {
 protected:
  void SetUp() override { memgraph::memory::EnableBackgroundThreads(); }

  void TearDown() override { memgraph::memory::EnableBackgroundThreads(); }
};

}  // namespace

TEST_F(PurgeUnusedMemoryTest, ReclaimsPagesWhileBackgroundThreadIsDecaying) {
  HeldPurgeArena arena;
  ASSERT_NO_FATAL_FAILURE(arena.Install());
  const auto decay_prefix = "arena." + std::to_string(arena.Index());
  // A short decay makes the background thread purge this arena promptly, and no muzzy stage
  // sends dirty pages straight to the dalloc hook.
  WriteMallctl<ssize_t>(decay_prefix + ".dirty_decay_ms", 1);
  WriteMallctl<ssize_t>(decay_prefix + ".muzzy_decay_ms", 0);

  std::vector<void *> objects;
  objects.reserve(kObjects * 2);
  for (int i = 0; i < kObjects * 2; ++i) objects.push_back(je_mallocx(kObjectBytes, arena.Flags()));

  for (int i = 0; i < kObjects; ++i) je_dallocx(objects[i], arena.Flags());
  ASSERT_TRUE(arena.WaitUntilHeld(10s)) << "the background thread never started decaying the arena";

  // Pages freed now are not in the held pass, which purges only what it collected when it started.
  for (int i = kObjects; i < kObjects * 2; ++i) je_dallocx(objects[i], arena.Flags());

  std::jthread releaser([&arena] {
    std::this_thread::sleep_for(200ms);
    arena.Release();
  });
  memgraph::memory::PurgeUnusedMemory();
  const auto unpurged = UnpurgedPages(arena.Index());
  releaser.join();

  EXPECT_EQ(unpurged, 0U);
}

TEST_F(PurgeUnusedMemoryTest, LeavesBackgroundThreadsAsItFoundThem) {
  memgraph::memory::PurgeUnusedMemory();
  EXPECT_TRUE(ReadMallctl<bool>("background_thread"));

  WriteMallctl("background_thread", false);
  memgraph::memory::PurgeUnusedMemory();
  EXPECT_FALSE(ReadMallctl<bool>("background_thread"));
}

#else

TEST(PurgeUnusedMemoryTest, RequiresJemalloc) { GTEST_SKIP() << "built without jemalloc"; }

#endif
