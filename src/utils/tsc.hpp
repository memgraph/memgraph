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
/// Stops the compiler moving code across a counter read. Emits no
/// instructions, so it constrains only compile-time scheduling; the reads
/// below are otherwise free to drift within the processor's execution window.
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

/// Read for the opening edge of a measured region. Ordered against the
/// surrounding code at compile time only: the processor may still retire
/// preceding work after the sample and charge a little of it to the region.
/// Serialising the read costs more than an indicative profile is worth.
inline uint64_t ReadTSCStart() noexcept {
  detail::CompilerBarrier();
  auto const ticks = ReadTSC();
  detail::CompilerBarrier();
  return ticks;
}

/// Read for the closing edge of a measured region. The region's own work has
/// completed before the counter is sampled, so none of it escapes into
/// whatever is measured next; work following the region may still be drawn in
/// ahead of the sample.
inline uint64_t ReadTSCEnd() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  detail::CompilerBarrier();
  // RDTSCP, unlike RDTSC, does not execute until preceding instructions have.
  uint32_t processor_id = 0;
  auto const ticks = __rdtscp(&processor_id);
  detail::CompilerBarrier();
  return ticks;
#elif defined(__aarch64__)
  // CNTVCT_EL0 has no counterpart that waits on preceding instructions, so the
  // wait has to be spelled out.
  asm volatile("isb" ::: "memory");
  auto const ticks = ReadTSC();
  detail::CompilerBarrier();
  return ticks;
#else
  return 0;
#endif
}

}  // namespace memgraph::utils
