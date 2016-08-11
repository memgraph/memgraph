#pragma once

#include <stdint.h>
#include <errno.h>
#include <atomic>

#include "threading/sync/lock_timeout_error.hpp"
#include "utils/cpu_relax.hpp"
#include "utils/sys.hpp"

class Futex
{
    using futex_t = uint32_t;
    using flag_t = uint8_t;

    /* @brief Data structure for implementing fast mutexes
     *
     * This structure is 4B wide, as required for futex system call where
     * the last two bytes are used for two flags - contended and locked,
     * respectively. Memory layout for the structure looks like this:
     *
     *                 all
     * |---------------------------------|
     * 00000000 00000000 0000000C 0000000L
     *                   |------| |------|
     *                  contended  locked
     *
     * L marks the locked bit
     * C marks the contended bit
     */
    union mutex_t
    {
        std::atomic<futex_t> all {0};

        struct
        {
            std::atomic<flag_t> locked;
            std::atomic<flag_t> contended;
        } state;
    };

    enum Contention : futex_t
    {
        UNCONTENDED = 0x0000,
        CONTENDED   = 0x0100
    };

    enum State : futex_t
    {
        UNLOCKED = 0x0000,
        LOCKED   = 0x0001,
        UNLOCKED_CONTENDED = UNLOCKED | CONTENDED, // 0x0100
        LOCKED_CONTENDED   = LOCKED   | CONTENDED  // 0x0101
    };

    static constexpr size_t LOCK_RETRIES = 100;
    static constexpr size_t UNLOCK_RETRIES = 200;

public:
    Futex()
    {
        static_assert(sizeof(mutex_t) == sizeof(futex_t),
            "Atomic futex should be the same size as non_atomic");
    }

    bool try_lock()
    {
        // we took the lock if we stored the LOCKED state and previous
        // state was UNLOCKED
        return mutex.state.locked.exchange(LOCKED, std::memory_order_acquire)
            == UNLOCKED;
    }

    void lock(const struct timespec* timeout = nullptr)
    {
        // try to fast lock a few times before going to sleep
        for(size_t i = 0; i < LOCK_RETRIES; ++i)
        {
            // try to lock and exit if we succeed
            if(try_lock())
                return;

            // we failed, chill a bit
            relax();
        }

        // the lock is contended, go to sleep. when someone
        // wakes you up, try taking the lock again
        while(mutex.all.exchange(LOCKED_CONTENDED, std::memory_order_acquire)
            & LOCKED)
        {
            // wait in the kernel for someone to wake us up when unlocking
            auto status = futex_wait(LOCKED_CONTENDED, timeout);

            // check if we woke up because of a timeout
            if(status == -1 && errno == ETIMEDOUT)
                throw LockTimeoutError("Lock timeout");
        }
    }

    void unlock()
    {
        futex_t state = LOCKED;

        // if we're locked and uncontended, try to unlock the mutex before
        // it becomes contended
        if(mutex.all.load(std::memory_order_acquire) == LOCKED &&
           mutex.all.compare_exchange_strong(state, UNLOCKED,
               std::memory_order_release,
               std::memory_order_relaxed))
            return;

        // we are contended, just release the lock
        mutex.state.locked.store(UNLOCKED, std::memory_order_release);

        // spin and hope someone takes a lock so we don't have to wake up
        // anyone because that's quite expensive
        for(size_t i = 0; i < UNLOCK_RETRIES; ++i)
        {
            // if someone took the lock, we're ok
            if(is_locked(std::memory_order_acquire))
                return;

            relax();
        }

        // store that we are becoming uncontended
        mutex.state.contended.store(UNCONTENDED, std::memory_order_release);

        // we need to wake someone up
        futex_wake(LOCKED);
    }

    bool is_locked(std::memory_order order = std::memory_order_seq_cst) const
    {
        return mutex.state.locked.load(order);
    }

    bool is_contended(std::memory_order order = std::memory_order_seq_cst) const
    {
        return mutex.state.contended.load(order);
    }

private:
    mutex_t mutex;

    int futex_wait(int value, const struct timespec* timeout = nullptr)
    {
        return sys::futex(&mutex.all, FUTEX_WAIT_PRIVATE, value,
                          timeout, nullptr, 0);
    }

    void futex_wake(int value)
    {
        sys::futex(&mutex.all, FUTEX_WAKE_PRIVATE, value, nullptr, nullptr, 0);
    }

    void relax()
    {
        cpu_relax();
    }
};

