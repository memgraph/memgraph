#pragma once

#include <vector>
#include <cassert>
#include <memory>
#include "storage/locking/lock_status.hpp"

namespace tx
{

template <class T>
class LockStore
{
    class LockHolder
    {
    public:
        LockHolder() noexcept = default;

        template <class... Args>
        LockHolder(T* lock, Args&&... args) noexcept : lock(lock)
        {
            assert(lock != nullptr);
            auto status = lock->lock(std::forward<Args>(args)...);

            if(status != LockStatus::Acquired)
                lock = nullptr;
        }

        LockHolder(const LockHolder&) = delete;
        LockHolder(LockHolder&& other) noexcept : lock(other.lock)
        {
            other.lock = nullptr;
        }

        ~LockHolder()
        {
            if(lock != nullptr)
                lock->unlock();
        }

        bool active() const { return lock != nullptr; }

    private:
        T* lock {nullptr};
    };

public:
    template <class... Args>
    void take(T* lock, Args&&... args)
    {
        auto holder = LockHolder(lock, std::forward<Args>(args)...);

        if(!holder.active())
            return;

        locks.emplace_back(LockHolder(lock, std::forward<Args>(args)...));
    }

private:
    std::vector<LockHolder> locks;
};

};
