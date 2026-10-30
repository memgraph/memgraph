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

#include <cstdint>
#ifdef MG_ENTERPRISE
#include <atomic>
#include <mutex>
#include <thread>
#include <vector>
#endif

#include "gtest/gtest.h"

#include "utils/pointer_pack.hpp"

using memgraph::utils::PointerPack;

namespace {

struct alignas(8) Dummy {
  uint64_t value{0};
};

}  // namespace

class PointerPackTest : public ::testing::Test {
 protected:
  Dummy a{1};
  Dummy b{2};
};

TEST_F(PointerPackTest, DefaultConstructedIsNull) {
  PointerPack<Dummy, 2> pp;
  EXPECT_EQ(pp.GetPtr(), nullptr);
  EXPECT_EQ(pp.Get<0>(), 0u);
  EXPECT_EQ(pp.Get<1>(), 0u);
}

TEST_F(PointerPackTest, ConstructWithPointerOnly) {
  PointerPack<Dummy, 2> pp(&a);
  EXPECT_EQ(pp.GetPtr(), &a);
  EXPECT_EQ(pp.Get<0>(), 0u);
  EXPECT_EQ(pp.Get<1>(), 0u);
}

TEST_F(PointerPackTest, ConstructWithPointerAndFlags) {
  PointerPack<Dummy, 3> pp(&a, 0b101);
  EXPECT_EQ(pp.GetPtr(), &a);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 0u);
  EXPECT_EQ(pp.Get<2>(), 1u);
}

TEST_F(PointerPackTest, SetAndGetIndividualBits) {
  PointerPack<Dummy, 3> pp(&a);

  pp.Set<0>(1);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 0u);
  EXPECT_EQ(pp.Get<2>(), 0u);
  EXPECT_EQ(pp.GetPtr(), &a);

  pp.Set<1>(1);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 1u);
  EXPECT_EQ(pp.Get<2>(), 0u);
  EXPECT_EQ(pp.GetPtr(), &a);

  pp.Set<2>(1);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 1u);
  EXPECT_EQ(pp.Get<2>(), 1u);
  EXPECT_EQ(pp.GetPtr(), &a);

  pp.Set<0>(0);
  EXPECT_EQ(pp.Get<0>(), 0u);
  EXPECT_EQ(pp.Get<1>(), 1u);
  EXPECT_EQ(pp.Get<2>(), 1u);
  EXPECT_EQ(pp.GetPtr(), &a);
}

TEST_F(PointerPackTest, MultiBitField) {
  PointerPack<Dummy, 3> pp(&a);

  pp.Set<0, 2>(0b11);
  EXPECT_EQ((pp.Get<0, 2>()), 0b11u);
  EXPECT_EQ(pp.Get<2>(), 0u);
  EXPECT_EQ(pp.GetPtr(), &a);

  pp.Set<0, 2>(0b10);
  EXPECT_EQ((pp.Get<0, 2>()), 0b10u);

  pp.Set<0, 3>(0b101);
  EXPECT_EQ((pp.Get<0, 3>()), 0b101u);
}

TEST_F(PointerPackTest, SetPtrPreservesFlags) {
  PointerPack<Dummy, 2> pp(&a);
  pp.Set<0>(1);
  pp.Set<1>(1);

  pp.SetPtr(&b);
  EXPECT_EQ(pp.GetPtr(), &b);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 1u);
}

TEST_F(PointerPackTest, SetFlagsPreservesPointer) {
  PointerPack<Dummy, 2> pp(&a);

  pp.Set<0>(1);
  EXPECT_EQ(pp.GetPtr(), &a);

  pp.Set<1>(1);
  EXPECT_EQ(pp.GetPtr(), &a);

  pp.Set<0>(0);
  EXPECT_EQ(pp.GetPtr(), &a);
}

TEST_F(PointerPackTest, ImplicitConversionToPointer) {
  PointerPack<Dummy, 2> pp(&a);
  pp.Set<0>(1);

  Dummy *raw = pp;
  EXPECT_EQ(raw, &a);
}

TEST_F(PointerPackTest, AssignmentFromPointer) {
  PointerPack<Dummy, 2> pp(&a);
  pp.Set<0>(1);
  pp.Set<1>(1);

  pp = &b;
  EXPECT_EQ(pp.GetPtr(), &b);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 1u);
}

TEST_F(PointerPackTest, NullPointerWithFlags) {
  PointerPack<Dummy, 2> pp(nullptr, 0b11);
  EXPECT_EQ(pp.GetPtr(), nullptr);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 1u);
}

TEST_F(PointerPackTest, ClearFlagsIndependently) {
  PointerPack<Dummy, 3> pp(&a, 0b111);

  pp.Set<1>(0);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 0u);
  EXPECT_EQ(pp.Get<2>(), 1u);
  EXPECT_EQ(pp.GetPtr(), &a);
}

TEST_F(PointerPackTest, FlagsExcessBitsMasked) {
  PointerPack<Dummy, 2> pp(&a);
  pp.Set<0>(0xFF);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.GetPtr(), &a);
}

TEST_F(PointerPackTest, ConstructorFlagsExcessBitsMasked) {
  PointerPack<Dummy, 2> pp(&a, 0xFF);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 1u);
  EXPECT_EQ(pp.GetPtr(), &a);
}

TEST_F(PointerPackTest, SingleBitPack) {
  PointerPack<Dummy, 1> pp(&a);
  EXPECT_EQ(pp.Get<0>(), 0u);

  pp.Set<0>(1);
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.GetPtr(), &a);

  pp.Set<0>(0);
  EXPECT_EQ(pp.Get<0>(), 0u);
  EXPECT_EQ(pp.GetPtr(), &a);
}

#ifdef MG_ENTERPRISE
namespace {

struct alignas(4) Align4 {
  uint32_t value{0};
};

template <typename T, int N>
concept Packable = requires { typename PointerPack<T, N>; };

}  // namespace

// The flag bits live in the pointer's alignment slack: 8-byte alignment gives exactly 3.
static_assert(Packable<Dummy, 3>);
static_assert(!Packable<Dummy, 4>);
static_assert(Packable<Align4, 2>);
static_assert(!Packable<Align4, 3>);

TEST_F(PointerPackTest, SetRelaxedLeavesOtherFlagsAndPointerUntouched) {
  PointerPack<Dummy, 3> pp(&a, 0b011);

  pp.SetRelaxed<2>(true);
  EXPECT_TRUE(pp.GetRelaxed<2>());
  EXPECT_EQ(pp.Get<0>(), 1u);
  EXPECT_EQ(pp.Get<1>(), 1u);
  EXPECT_EQ(pp.GetPtr(), &a);

  pp.Set<0>(0);
  pp.SetPtr(&b);
  EXPECT_TRUE(pp.GetRelaxed<2>());
  EXPECT_FALSE(pp.GetRelaxed<0>());
  EXPECT_TRUE(pp.GetRelaxed<1>());

  pp.SetRelaxed<2>(false);
  EXPECT_FALSE(pp.GetRelaxed<2>());
  EXPECT_EQ(pp.Get<0>(), 0u);
  EXPECT_EQ(pp.Get<1>(), 1u);
  EXPECT_EQ(pp.GetPtr(), &b);

  pp.Set<0, 2>(0b10);
  EXPECT_EQ((pp.Get<0, 2>()), 0b10u);
  EXPECT_FALSE(pp.GetRelaxed<2>());
  EXPECT_EQ(pp.GetPtr(), &b);
}

// Lock-free readers of one bit race with writers that hold a lock and rewrite the pointer and the other
// flags. Under TSan any non-atomic write to the packed word is reported; in every build, once set the
// bit must never be observed cleared.
TEST_F(PointerPackTest, RelaxedReaderVsLockedWriters) {
  constexpr int kWriters = 2;
  constexpr int kIterations = 20'000;

  PointerPack<Dummy, 3> pp(&a);
  std::mutex lock;
  std::atomic<bool> done{false};
  std::atomic<bool> reader_saw_clear_after_set{false};

  std::thread reader([&] {
    bool seen_set = false;
    while (!done.load(std::memory_order_acquire)) {
      bool const now = pp.GetRelaxed<2>();
      if (seen_set && !now) reader_saw_clear_after_set.store(true, std::memory_order_relaxed);
      seen_set = seen_set || now;
    }
  });

  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      for (int i = 0; i < kIterations; ++i) {
        auto guard = std::lock_guard{lock};
        pp.Set<0>(i & 1);
        pp.Set<1>((i >> 1) & 1);
        pp.SetPtr((i & 1) != 0 ? &a : &b);
        if (w == 0 && i == kIterations / 2) pp.SetRelaxed<2>(true);
      }
    });
  }
  for (auto &t : writers) t.join();
  done.store(true, std::memory_order_release);
  reader.join();

  EXPECT_FALSE(reader_saw_clear_after_set.load());
  auto guard = std::lock_guard{lock};
  EXPECT_TRUE(pp.GetRelaxed<2>());
}
#endif  // MG_ENTERPRISE
