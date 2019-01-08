#pragma once

#include <sys/prctl.h>
#include <x86intrin.h>

namespace utils {

// TSC stands for Time-Stamp Counter

#ifndef __x86_64__
#error The TSC library only supports x86_64
#endif

/// This function reads the CPUs internal Time-Stamp Counter. This counter is
/// used to get a precise timestamp. It differs from the usual POSIX
/// `clock_gettime` in that (without additional computing) it can be only used
/// for relative time measurements. The Linux kernel uses the TSC internally to
/// implement `clock_gettime` when the clock source is set to TSC.
///
/// The TSC is implemented as a register in the CPU that increments its value
/// with a constant rate. The behavior of the TSC initially was to increment the
/// counter on each instruction. This was then changed to the current behavior
/// (>= Pentium 4) that increments the TSC with a constant rate so that CPU
/// frequency scaling doesn't affect the measurements.
///
/// The TSC is guaranteed that it won't overflow in 10 years and because it is
/// mostly implemented as a 64bit register it doesn't overflow even in 190 years
/// (see Intel manual).
///
/// One issue of the TSC is that it is unique for each logical CPU. That means
/// that if a context switch happens between two `ReadTSC` calls it could mean
/// that the counters could have a very large offset. Thankfully, the counters
/// are reset to 0 at each CPU reset and the Linux kernel synchronizes all of
/// the counters. The synchronization can be seen here (`tsc_verify_tsc_adjust`
/// and `tsc_store_and_check_tsc_adjust`):
/// https://github.com/torvalds/linux/blob/master/arch/x86/kernel/tsc_sync.c
/// https://github.com/torvalds/linux/blob/master/arch/x86/kernel/tsc.c
/// https://github.com/torvalds/linux/blob/master/arch/x86/include/asm/tsc.h
///
/// All claims here were taken from sections 17.17 and 2.8.6 of the Intel 64 and
/// IA-32 Architectures Software Developer's Manual.
///
/// Here we use the RDTSCP instruction instead of the RDTSC instruction because
/// the RDTSCP instruction forces serialization of code execution in the CPU.
/// Intel recommends the usage of the RDTSCP instruction for precision timing:
/// https://www.intel.com/content/dam/www/public/us/en/documents/white-papers/ia-32-ia-64-benchmark-code-execution-paper.pdf
///
/// Inline assembly MUST NOT be used because the compiler won't be aware that
/// certain registers are being overwritten by the RDTSCP instruction. That is
/// why we use the builtin command.
/// https://stackoverflow.com/questions/13772567/get-cpu-cycle-count/51907627#51907627
///
/// Comparison of hardware time sources can be seen here:
/// https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_MRG/2/html/Realtime_Reference_Guide/chap-Timestamping.html#example-Hardware_Clock_Cost_Comparison
inline unsigned long long ReadTSC() {
  unsigned int cpuid;
  return __rdtscp(&cpuid);
}

/// The TSC can be disabled using a flag in the CPU. This function checks for
/// the availability of the TSC. If you call `ReadTSC` when the TSC isn't
/// available it will cause a segmentation fault.
/// https://blog.cr0.org/2009/05/time-stamp-counter-disabling-oddities.html
inline bool CheckAvailableTSC() {
  int ret;
  if (prctl(PR_GET_TSC, &ret) != 0) return false;
  return ret == PR_TSC_ENABLE;
}

}  // namespace utils
