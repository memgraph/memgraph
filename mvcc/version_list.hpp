#pragma once

#include "threading/sync/lockable.hpp"
#include "transactions/transaction.hpp"

#include "memory/lazy_gc.hpp"
#include "mvcc/mvcc_error.hpp"

namespace mvcc
{

template <class T>
class VersionList : Lockable<SpinLock>, LazyGC<VersionList<T>>
{
public:
    class Accessor
    {

    };

    VersionList(const tx::Transaction& t)
    {
        // create a first version of the record
        auto v1 = new T();

        // mark the record as created by the transaction t
        v1->mark_created(t);

        head.store(v1, std::memory_order_release);
    }

private:
    std::atomic<T*> head;

    T* find(const tx::Transaction& t)
    {
        auto r = head.load(std::memory_order_acquire);

        //    nullptr
        //       |
        //     [v1]      ...
        //       |
        //     [v2] <------+
        //       |         |
        //     [v3] <------+
        //       |         |  Jump backwards until you find a first visible
        //   [VerList] ----+  version, or you reach the end of the list
        //
        while(r != nullptr && !r->visible(t))
            r = r->next.load(std::memory_order_acquire);

        return r;
    }

    T* update(const tx::Transaction& t)
    {



    }

    bool remove(const tx::Transaction& t)
    {
        // take a lock on this node
        auto guard = this->acquire();

        // find the visible record version to delete
        auto r = find(t);

        // exit if we haven't found any visible records
        if(r == nullptr)
            return false;

        // if the record hasn't been deleted yet, it's ok to delete it
        if(!r->tx.max())
        {
            // mark the record as deleted
            r->mark_deleted(t);
            return true;
        }

        auto hints = r->hints.load(std::memory_order_acquire);

        if(hints.max.is_committed() || hints.max.is_unknown())
            throw MvccError("Can't serialize");

        //...
    }

    void vacuum()
    {

    }
};

}
