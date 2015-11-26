#pragma once

#include <stdint.h>
#include <errno.h>
#include <atomic>

#include "lock_timeout_error.hpp"
#include "utils/cpu_relax.hpp"
#include "utils/sys.hpp"

class Futex
{
    using futex_t = int32_t;

    union mutex_t
    {
        std::atomic<futex_t> all {0};

        struct
        {
            std::atomic<uint8_t> locked;
            std::atomic<uint8_t> contended;
        } state;
    };

    enum Contension : futex_t
    {
        UNCONTENDED = 0x0000,
        CONTENDED   = 0x0100
    };

    enum State : futex_t
    {
        UNLOCKED = 0x0000,
        LOCKED   = 0x0001,
        UNLOCKED_CONTENDED = UNLOCKED | CONTENDED,
        LOCKED_CONTENDED   = LOCKED   | CONTENDED
    };

    static constexpr size_t LOCK_RETRIES = 256;
    static constexpr size_t UNLOCK_RETRIES = 512;

public:
    Futex()
    {
        static_assert(sizeof(mutex_t) == sizeof(futex_t),
            "Atomic futex should be the same size as non_atomic");
    }

    bool try_lock()
    {
        return mutex.state.locked.exchange(LOCKED, std::memory_order_acquire)
            == UNLOCKED;
    }

    void lock(const struct timespec* timeout = nullptr)
    {
        // try to fast lock a few times before going to sleep
        for(size_t i = 0; i < LOCK_RETRIES; ++i)
        {
            if(try_lock())
                return;

            // we failed, chill a bit
            cpu_relax();
        }

        // the lock is contended, go to sleep. when someone
        // wakes you up, try taking the lock again
        while(mutex.all.exchange(LOCKED_CONTENDED, std::memory_order_acquire)
            & LOCKED)
        {
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
        mutex.state.locked.store(UNLOCKED, std::memory_order_seq_cst);

        // spin and hope someone takes a lock so we don't have to wake up
        // anyone because that's quite expensive
        for(size_t i = 0; i < UNLOCK_RETRIES; ++i)
        {
            if(mutex.state.locked.load(std::memory_order_acquire) & LOCKED)
                return;

            cpu_relax();
        }

        // we need to wake someone up
        mutex.state.contended.store(UNCONTENDED, std::memory_order_release);
        futex_wake(LOCKED);
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
};

