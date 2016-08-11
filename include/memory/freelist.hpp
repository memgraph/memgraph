#pragma once

#include <vector>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

template <class T, class lock_t = SpinLock>
class FreeList : Lockable<lock_t>
{
public:
    void swap(std::vector<T *> &dst) { std::swap(data, dst); }

    void add(T *element)
    {
        auto lock = this->acquire_unique();
        data.emplace_back(element);
    }

    size_t size() const
    {
        return data.size();
    }

private:
    std::vector<T *> data;
};
