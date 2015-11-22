#pragma once

#include <atomic>

#include "utils/crtp.hpp"
#include "threading/sync/lockable.hpp"

template <class Derived>
class LazyGC : Crtp<Derived>, Lockable<SpinLock>
{
public:
    void add_ref()
    {
        ref_count.fetch_add(1, std::memory_order_relaxed);
    }

    void release_ref()
    {
        // get refcount and subtract atomically
        auto count = ref_count.fetch_sub(1, std::memory_order_acq_rel);

        // fetch_sub first returns and then subtrarcts so the refcount is
        // zero when fetch_sub returns 1
        if(count != 1)
            return;

        auto guard = this->aacquire();

        this->derived().vacuum();
    }

private:
    std::atomic<int> ref_count {0};
};
