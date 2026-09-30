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

#include "memory/db_arena.hpp"
#include "memory/global_memory_control.hpp"

#if USE_JEMALLOC

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstddef>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <jemalloc/jemalloc.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

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

void RefreshStats() {
  uint64_t epoch = 1;
  size_t size = sizeof(epoch);
  EXPECT_EQ(je_mallctl("epoch", &epoch, &size, &epoch, size), 0);
}

size_t UnpurgedPages(unsigned arena) {
  RefreshStats();
  const auto prefix = "stats.arenas." + std::to_string(arena);
  return ReadMallctl<size_t>(prefix + ".pdirty") + ReadMallctl<size_t>(prefix + ".pmuzzy");
}

// While in scope, this process cannot create threads: the per-user process limit is set below what
// the user already runs. A user who may exceed the limit, such as root, is unaffected, which
// Refusing() reports.
class ThreadCreationRefused {
 public:
  ThreadCreationRefused() {
    EXPECT_EQ(getrlimit(RLIMIT_NPROC, &saved_), 0);
    const rlimit refused{.rlim_cur = 1, .rlim_max = saved_.rlim_max};
    EXPECT_EQ(setrlimit(RLIMIT_NPROC, &refused), 0);
  }

  ThreadCreationRefused(const ThreadCreationRefused &) = delete;
  ThreadCreationRefused &operator=(const ThreadCreationRefused &) = delete;
  ThreadCreationRefused(ThreadCreationRefused &&) = delete;
  ThreadCreationRefused &operator=(ThreadCreationRefused &&) = delete;

  ~ThreadCreationRefused() { EXPECT_EQ(setrlimit(RLIMIT_NPROC, &saved_), 0); }

  static bool Refusing() {
    pthread_t thread{};
    if (pthread_create(&thread, nullptr, [](void *) -> void * { return nullptr; }, nullptr) != 0) return true;
    pthread_join(thread, nullptr);
    return false;
  }

 private:
  rlimit saved_{};
};

// Runs a purge whose restart of the background threads fails. Returns false if thread creation
// could not be refused, in which case nothing was purged.
bool PurgeWithThreadCreationRefused() {
  const ThreadCreationRefused refused;
  if (!ThreadCreationRefused::Refusing()) return false;
  memgraph::memory::PurgeUnusedMemory();
  return true;
}

// An arena whose dirty pages become due for purging a millisecond after they are freed, and are
// then purged outright.
class DecayingArena {
 public:
  DecayingArena() = default;

  DecayingArena(const DecayingArena &) = delete;
  DecayingArena &operator=(const DecayingArena &) = delete;
  DecayingArena(DecayingArena &&) = delete;
  DecayingArena &operator=(DecayingArena &&) = delete;

  ~DecayingArena() {
    if (created_) EXPECT_EQ(je_mallctl(Name("destroy").c_str(), nullptr, nullptr, nullptr, 0), 0);
  }

  // Call through ASSERT_NO_FATAL_FAILURE, so a test never changes the decay of an arena it did not create.
  void Create() {
    size_t size = sizeof(arena_);
    ASSERT_EQ(je_mallctl("arenas.create", &arena_, &size, nullptr, 0), 0);
    created_ = true;
    WriteMallctl<ssize_t>(Name("dirty_decay_ms"), 1);
    WriteMallctl<ssize_t>(Name("muzzy_decay_ms"), 0);
  }

  int Flags() const { return MALLOCX_ARENA(arena_) | MALLOCX_TCACHE_NONE; }

  uint64_t PurgedPages() const {
    RefreshStats();
    return ReadMallctl<uint64_t>("stats.arenas." + std::to_string(arena_) + ".dirty_purged");
  }

 private:
  std::string Name(const char *leaf) const { return "arena." + std::to_string(arena_) + "." + leaf; }

  unsigned arena_{0};
  bool created_{false};
};

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

// An arena whose extent hooks stall the thread that destroys it in the first hook that thread
// enters. jemalloc pauses the arena's background thread for the whole destroy, so the stall gives
// that thread time to wake and find itself paused, and another thread time to act while the destroy
// is under way. The hooks find the arena through a static pointer, so only one may exist at a time.
// Failures end the process, which runs as a forked child.
class SlowlyDestroyedArena {
 public:
  SlowlyDestroyedArena() {
    instance_ = this;
    size_t size = sizeof(arena_);
    Require(je_mallctl("arenas.create", &arena_, &size, nullptr, 0) == 0);
    size = sizeof(base_);
    Require(je_mallctl(Name("extent_hooks").c_str(), &base_, &size, nullptr, 0) == 0);
    hooks_ = *base_;
    if (hooks_.dalloc != nullptr) hooks_.dalloc = &Dalloc;
    if (hooks_.destroy != nullptr) hooks_.destroy = &Destroy;
    if (hooks_.purge_lazy != nullptr) hooks_.purge_lazy = &PurgeLazy;
    if (hooks_.purge_forced != nullptr) hooks_.purge_forced = &PurgeForced;
    extent_hooks_t *mine = &hooks_;
    Require(je_mallctl(Name("extent_hooks").c_str(), nullptr, nullptr, &mine, sizeof(mine)) == 0);
  }

  SlowlyDestroyedArena(const SlowlyDestroyedArena &) = delete;
  SlowlyDestroyedArena &operator=(const SlowlyDestroyedArena &) = delete;
  SlowlyDestroyedArena(SlowlyDestroyedArena &&) = delete;
  SlowlyDestroyedArena &operator=(SlowlyDestroyedArena &&) = delete;
  ~SlowlyDestroyedArena() = default;

  static void Require(bool ok) {
    if (!ok) _exit(1);
  }

  int Flags() const { return MALLOCX_ARENA(arena_) | MALLOCX_TCACHE_NONE; }

  std::string Name(const char *leaf) const { return "arena." + std::to_string(arena_) + "." + leaf; }

  void Destroy() {
    destroyer_ = std::this_thread::get_id();
    stall_.store(true);
    Require(je_mallctl(Name("destroy").c_str(), nullptr, nullptr, nullptr, 0) == 0);
  }

  void WaitUntilStalled() const { stalled_.wait(false); }

 private:
  static void MaybeStall() {
    if (std::this_thread::get_id() == instance_->destroyer_ && instance_->stall_.exchange(false)) {
      instance_->stalled_.store(true);
      instance_->stalled_.notify_all();
      std::this_thread::sleep_for(1s);
    }
  }

  static bool Dalloc(extent_hooks_t * /*hooks*/, void *addr, size_t size, bool committed, unsigned arena_ind) {
    MaybeStall();
    return instance_->base_->dalloc(instance_->base_, addr, size, committed, arena_ind);
  }

  static void Destroy(extent_hooks_t * /*hooks*/, void *addr, size_t size, bool committed, unsigned arena_ind) {
    MaybeStall();
    instance_->base_->destroy(instance_->base_, addr, size, committed, arena_ind);
  }

  static bool PurgeLazy(extent_hooks_t * /*hooks*/, void *addr, size_t size, size_t offset, size_t length,
                        unsigned arena_ind) {
    MaybeStall();
    return instance_->base_->purge_lazy(instance_->base_, addr, size, offset, length, arena_ind);
  }

  static bool PurgeForced(extent_hooks_t * /*hooks*/, void *addr, size_t size, size_t offset, size_t length,
                          unsigned arena_ind) {
    MaybeStall();
    return instance_->base_->purge_forced(instance_->base_, addr, size, offset, length, arena_ind);
  }

  static inline SlowlyDestroyedArena *instance_ = nullptr;

  unsigned arena_{0};
  extent_hooks_t *base_{nullptr};
  extent_hooks_t hooks_{};
  std::thread::id destroyer_;
  std::atomic<bool> stall_{false};
  std::atomic<bool> stalled_{false};
};

// Purges while an arena is being destroyed and its background thread keeps waking. With a single
// background thread, thread 0 serves the arena, and pages still decaying keep it sleeping on a
// short timer rather than indefinitely. The purge is issued before the thread first wakes during
// the destroy, so it is first in line when the destroy ends.
void DestroyArenaThenPurge() {
  size_t one = 1;
  SlowlyDestroyedArena::Require(je_mallctl("max_background_threads", nullptr, nullptr, &one, sizeof(one)) == 0);
  memgraph::memory::EnableBackgroundThreads();

  SlowlyDestroyedArena arena;
  ssize_t decay_ms = 1000;
  SlowlyDestroyedArena::Require(
      je_mallctl(arena.Name("dirty_decay_ms").c_str(), nullptr, nullptr, &decay_ms, sizeof(decay_ms)) == 0);
  std::vector<void *> objects;
  objects.reserve(kObjects);
  for (int i = 0; i < kObjects; ++i) objects.push_back(je_mallocx(kObjectBytes, arena.Flags()));
  for (void *object : objects) je_dallocx(object, arena.Flags());
  // An application thread checks an arena's decay only once enough of its calls in the arena have
  // passed, and the first check after the frees wakes the background thread, which then keeps
  // waking while the freed pages decay.
  const auto deadline = std::chrono::steady_clock::now() + 200ms;
  while (std::chrono::steady_clock::now() < deadline) {
    je_dallocx(je_mallocx(kObjectBytes, arena.Flags()), arena.Flags());
  }

  std::jthread purger([&arena] {
    arena.WaitUntilStalled();
    memgraph::memory::PurgeUnusedMemory();
  });
  arena.Destroy();
}

// Background threads are process-wide, so every test starts with them running and leaves them
// running however it ends. The arenas a test creates must also be ones no thread is bound to by the
// CPU it runs on, as the server ensures at startup.
class PurgeUnusedMemoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::memory::EnsureCpuArenaCoverage();
    memgraph::memory::EnableBackgroundThreads();
  }

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

TEST_F(PurgeUnusedMemoryTest, CompletesRightAfterAnArenaIsDestroyed) {
  // A deadlock cannot be undone in this process, so the purge runs in a child that is killed if it
  // does not finish.
  const pid_t child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    DestroyArenaThenPurge();
    _exit(0);
  }

  int status = 0;
  pid_t waited = 0;
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while ((waited = waitpid(child, &status, WNOHANG)) == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }
  if (waited == 0) {
    kill(child, SIGKILL);
    waitpid(child, &status, 0);
    FAIL() << "the purge did not finish";
  }
  ASSERT_EQ(waited, child);
  EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0) << "child status " << status;
}

TEST_F(PurgeUnusedMemoryTest, FailedRestartLeavesBackgroundThreadsOff) {
  if (!PurgeWithThreadCreationRefused()) GTEST_SKIP() << "this user can create threads past the process limit";
  EXPECT_FALSE(ReadMallctl<bool>("background_thread"));
}

TEST_F(PurgeUnusedMemoryTest, FailedRestartLeavesFreedPagesDecaying) {
  if (!PurgeWithThreadCreationRefused()) GTEST_SKIP() << "this user can create threads past the process limit";

  DecayingArena arena;
  ASSERT_NO_FATAL_FAILURE(arena.Create());

  // Decay runs on a thread that allocates or frees in the arena, once enough such calls have
  // passed; these calls are what give the application thread its turn.
  const auto deadline = std::chrono::steady_clock::now() + 200ms;
  while (std::chrono::steady_clock::now() < deadline) {
    je_dallocx(je_mallocx(kObjectBytes, arena.Flags()), arena.Flags());
  }
  EXPECT_GT(arena.PurgedPages(), 0U);
}

TEST_F(PurgeUnusedMemoryTest, BackgroundThreadsCanBeEnabledAfterAFailedRestart) {
  if (!PurgeWithThreadCreationRefused()) GTEST_SKIP() << "this user can create threads past the process limit";
  memgraph::memory::EnableBackgroundThreads();

  DecayingArena arena;
  ASSERT_NO_FATAL_FAILURE(arena.Create());

  // With background threads enabled an application thread does not purge dirty pages: once enough
  // of its calls in the arena have passed, it wakes the arena's background thread instead. Pages
  // purged from this arena were therefore purged by a background thread.
  std::vector<void *> objects;
  objects.reserve(kObjects);
  for (int i = 0; i < kObjects; ++i) objects.push_back(je_mallocx(kObjectBytes, arena.Flags()));
  for (void *object : objects) je_dallocx(object, arena.Flags());

  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (arena.PurgedPages() == 0 && std::chrono::steady_clock::now() < deadline) {
    je_dallocx(je_mallocx(kObjectBytes, arena.Flags()), arena.Flags());
  }
  EXPECT_GT(arena.PurgedPages(), 0U);
}

#else

TEST(PurgeUnusedMemoryTest, RequiresJemalloc) { GTEST_SKIP() << "built without jemalloc"; }

#endif
