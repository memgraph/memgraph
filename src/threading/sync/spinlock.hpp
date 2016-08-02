#pragma once

#include <atomic>
#include <unistd.h>

#include "utils/cpu_relax.hpp"

class SpinLock
{
public:
    void lock()
    { // Before was memorz_order_acquire
        while (lock_flag.test_and_set(std::memory_order_seq_cst))
            cpu_relax();
        /// usleep(250);
    }
    // Before was memory_order_release
    void unlock() { lock_flag.clear(std::memory_order_seq_cst); }

private:
    // guaranteed by standard to be lock free!
    mutable std::atomic_flag lock_flag = ATOMIC_FLAG_INIT;
};
