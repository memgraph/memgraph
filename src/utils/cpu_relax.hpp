#pragma once

/* @brief improves contention in spinlocks
 * hints the processor that we're in a spinlock and not doing much
 */
inline void cpu_relax() {
  // if IBMPower
  // HMT_very_low()
  // http://stackoverflow.com/questions/5425506/equivalent-of-x86-pause-instruction-for-ppc
  asm("PAUSE");
}
