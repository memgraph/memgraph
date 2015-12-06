#pragma once

#include "threading/sync/futex.hpp"
#include "lock_status.hpp"
#include "mvcc/id.hpp"

class RecordLock
{
    static constexpr struct timespec timeout {20, 0};
    static constexpr Id INVALID = Id();

public:
    void lock()
    {
        mutex.lock(&timeout);
    }

    LockStatus lock(const Id& id)
    {
        if(mutex.try_lock())
            return owner = id, LockStatus::Acquired;

        if(owner == id)
            return LockStatus::AlreadyHeld;

        return mutex.lock(&timeout), LockStatus::Acquired;
    }

    void unlock()
    {
        // INVALID flag is far more readable, but  _ /
        // for some reason it gives linker errors  o.O
        // owner = INVALID;                        (_)
        owner = Id(); //                           ^ ^
        mutex.unlock();
    }

private:
    Futex mutex;
    Id owner;
};
