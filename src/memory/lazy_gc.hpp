#pragma once

#include <atomic>

#include "threading/sync/lockable.hpp"
#include "utils/crtp.hpp"

template <class Derived, class lock_t = SpinLock>
class LazyGC : public Crtp<Derived>, public Lockable<lock_t>
{
public:
    // add_ref method should be called by a thread
    // when the thread has to do something over
    // object which has to be lazy cleaned when
    // the thread finish it job
    void add_ref()
    {
        auto lock = this->acquire_unique();
        ++count;
    }

protected:
    size_t count{0};
};
