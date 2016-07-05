#include "storage/locking/record_lock.hpp"

void RecordLock::lock()
{
    mutex.lock(&timeout);
}

LockStatus RecordLock::lock(const Id& id)
{
    if(mutex.try_lock())
        return owner = id, LockStatus::Acquired;

    if(owner == id)
        return LockStatus::AlreadyHeld;

    return mutex.lock(&timeout), LockStatus::Acquired;
}

void RecordLock::unlock()
{
    owner = INVALID;
    mutex.unlock();
}

constexpr struct timespec RecordLock::timeout;
constexpr Id RecordLock::INVALID;

