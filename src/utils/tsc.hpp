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

#pragma once

#include <cstdint>

#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif

namespace memgraph::utils {

// TSC stands for Time-Stamp Counter, a free-running counter the CPU increments
// at a fixed rate. Reading it is far cheaper than a clock_gettime, at the cost
// of yielding ticks rather than a duration: only the difference between two
// reads on the same machine carries meaning.

/// Whether the counter ticks at a rate independent of core frequency scaling
/// and halt states, and whether every read this header emits is supported.
/// Deltas are comparable between samples only when this holds.
bool IsAvailableTSC();

namespace detail {
/// Stops the compiler moving code across the counter read. The fences below
/// constrain the processor only; without this the read could be scheduled
/// outside the region at compile time.
inline void CompilerBarrier() noexcept { asm volatile("" ::: "memory"); }
}  // namespace detail

/// Read with no ordering guarantee. The processor may execute it out of order
/// with respect to surrounding work, so it carries meaning only for samples
/// taken strictly inside a region already bracketed by ReadTSCStart and
/// ReadTSCEnd.
inline uint64_t ReadTSC() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  return __rdtsc();
#elif defined(__aarch64__)
  uint64_t ticks = 0;
  asm volatile("mrs %0, cntvct_el0" : "=r"(ticks));
  return ticks;
#else
  return 0;
#endif
}

/// Read for the opening edge of a measured region: work preceding the region
/// has completed before the counter is sampled, so it cannot be charged to the
/// region.
inline uint64_t ReadTSCStart() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  // LFENCE does not itself execute until everything before it has completed,
  // and dispatches nothing after it until it does.
  detail::CompilerBarrier();
  _mm_lfence();
  auto const ticks = __rdtsc();
  detail::CompilerBarrier();
  return ticks;
#elif defined(__aarch64__)
  // A CNTVCT_EL0 read is not ordered against neighbouring instructions on its
  // own; ISB is what the architecture offers to pin it down.
  asm volatile("isb" ::: "memory");
  auto const ticks = ReadTSC();
  detail::CompilerBarrier();
  return ticks;
#else
  return 0;
#endif
}

/// Read for the closing edge of a measured region: the region's work has
/// completed before the counter is sampled, and work following the region
/// cannot start before it.
inline uint64_t ReadTSCEnd() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  // RDTSCP supplies the leading half of the ordering by waiting on preceding
  // instructions; LFENCE supplies the trailing half.
  detail::CompilerBarrier();
  uint32_t processor_id = 0;
  auto const ticks = __rdtscp(&processor_id);
  _mm_lfence();
  detail::CompilerBarrier();
  return ticks;
#elif defined(__aarch64__)
  asm volatile("isb" ::: "memory");
  auto const ticks = ReadTSC();
  asm volatile("isb" ::: "memory");
  return ticks;
#else
  return 0;
#endif
}

}  // namespace memgraph::utils
