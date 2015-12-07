#pragma once

#include "threading/sync/lockable.hpp"
#include "transactions/transaction.hpp"

#include "memory/lazy_gc.hpp"
#include "mvcc/serialization_error.hpp"
#include "database/locking/record_lock.hpp"

namespace mvcc
{

template <class T>
class VersionList : LazyGC<VersionList<T>>
{
    friend class Accessor;

public:
    class Accessor
    {
        friend class VersionList<T>;

        Accessor(tx::Transaction& transaction, VersionList<T>& record)
            : transaction(transaction), record(record)
        {
            record.add_ref();
        }

        ~Accessor()
        {
            record.release_ref();
        }

    public:
        Accessor(const Accessor&) = default;
        Accessor(Accessor&&) = default;

        Accessor& operator=(const Accessor&) = delete;
        Accessor& operator=(Accessor&&) = delete;

        T* insert()
        {
            return record.insert(transaction);
        }

        const T* find() const
        {
            return record.find(transaction);
        }

        T* update()
        {
            return record.update(transaction);
        }

        bool remove()
        {
            return record.remove(transaction);
        }

    private:
        tx::Transaction& transaction;
        VersionList<T>& record;
    };

    VersionList() = default;

    VersionList(const VersionList&) = delete;
    VersionList(VersionList&&) = delete;

    Accessor access(tx::Transaction& transaction)
    {
        return Accessor(transaction, *this);
    }

    friend std::ostream& operator<<(std::ostream& stream,
                                    const VersionList<T>& vlist)
    {
        stream << "VersionList" << std::endl;

        auto record = vlist.head.load();

        while(record != nullptr)
        {
            stream << "-- " << *record << std::endl;
            record = record->next();
        }

        return stream;
    }

private:
    std::atomic<T*> head {nullptr};
    RecordLock lock;
    //static Recycler recycler;

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
            r = r->next(std::memory_order_acquire);

        return r;
    }

    T* insert(tx::Transaction& t)
    {
        assert(head == nullptr);

        // create a first version of the record
        // TODO replace 'new' with something better
        auto v1 = new T();

        // mark the record as created by the transaction t
        v1->mark_created(t);

        head.store(v1, std::memory_order_release);

        return v1;
    }

    T* update(tx::Transaction& t)
    {
        assert(head != nullptr);
        auto record = lock_and_validate(t);

        // check if we found any visible records
        if(!record)
            return nullptr;

        auto updated = new T();
        updated->data = record->data;

        updated->mark_created(t);
        record->mark_deleted(t);

        updated->next(record, std::memory_order_release);
        head.store(updated, std::memory_order_release);

        return updated;
    }

    bool remove(tx::Transaction& t)
    {
        assert(head != nullptr);
        auto record = lock_and_validate(t);

        if(!record)
            return false;

        return record->mark_deleted(t), true;
    }

    T* lock_and_validate(T* record, tx::Transaction& t)
    {
        assert(record != nullptr);
        assert(record == find(t));

        // take a lock on this node
        t.take_lock(lock);

        // if the record hasn't been deleted yet or the deleting transaction
        // has aborted, it's ok to modify it
        if(!record->tx.exp() || record->hints.load().exp.is_aborted())
            return record;

        // if it committed, then we have a serialization conflict
        assert(record->hints.load().exp.is_committed());
        throw SerializationError();
    }

    T* lock_and_validate(tx::Transaction& t)
    {
        // find the visible record version to delete
        auto record = find(t);
        return record == nullptr ? nullptr : lock_and_validate(record, t);
    }

    auto gc_lock_acquire()
    {
        return std::unique_lock<RecordLock>(lock);
    }

    void vacuum()
    {

    }
};

}

// these are convenient typedefs to use in other contexes to make the code a
// bit clearer
class Vertex;
class Edge;

using VertexRecord = mvcc::VersionList<Vertex>;
using EdgeRecord = mvcc::VersionList<Edge>;

