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
        owner = INVALID;
        mutex.unlock();
    }

private:
    Futex mutex;
    Id owner;
};

constexpr struct timespec RecordLock::timeout;
constexpr Id RecordLock::INVALID;

