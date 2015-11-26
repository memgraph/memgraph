#pragma once

/* @brief improves contention in spinlocks
 * hints the processor that we're in a spinlock and not doing much
 */
inline void cpu_relax()
{
    asm("PAUSE");
}
