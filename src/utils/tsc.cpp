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

#include "utils/tsc.hpp"

#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#endif

namespace memgraph::utils {

namespace {

bool DetectTSC() {
#if defined(__x86_64__) || defined(__i386__)
  uint32_t eax = 0;
  uint32_t ebx = 0;
  uint32_t ecx = 0;
  uint32_t edx = 0;

  constexpr uint32_t kExtendedFeatures = 0x80000001;
  constexpr uint32_t kRDTSCPBit = 1U << 27U;
  if (__get_cpuid(kExtendedFeatures, &eax, &ebx, &ecx, &edx) == 0) return false;
  if ((edx & kRDTSCPBit) == 0) return false;

  constexpr uint32_t kPowerManagement = 0x80000007;
  constexpr uint32_t kInvariantTSCBit = 1U << 8U;
  if (__get_cpuid(kPowerManagement, &eax, &ebx, &ecx, &edx) == 0) return false;
  return (edx & kInvariantTSCBit) != 0;
#elif defined(__aarch64__)
  // The generic timer is architectural from ARMv8 onwards, and its counter is
  // specified to run at a constant frequency.
  return true;
#else
  return false;
#endif
}

}  // namespace

bool IsAvailableTSC() {
  static bool const available = DetectTSC();
  return available;
}

}  // namespace memgraph::utils
