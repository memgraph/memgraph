#pragma once

#include "threading/sync/lockable.hpp"
#include "transactions/transaction.hpp"

#include "memory/lazy_gc.hpp"

namespace mvcc
{

template <class T>
class VersionList : Lockable<SpinLock>, LazyGC<VersionList<T>>
{
public:
    class Accessor
    {

    };

    VersionList() = default;

private:
    std::atomic<T*> head;

    void vacuum()
    {

    }
};

}
