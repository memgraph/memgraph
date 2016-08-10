#pragma once

#include "threading/sync/lockable.hpp"
#include "transactions/transaction.hpp"

#include "memory/lazy_gc.hpp"
#include "mvcc/serialization_error.hpp"
#include "storage/locking/record_lock.hpp"

namespace mvcc
{

template <class T>
class VersionList : public LazyGC<VersionList<T>>
{
    friend class Accessor;

public:
    using uptr = std::unique_ptr<VersionList<T>>;
    using item_t = T;

    VersionList(Id id) : id(id) {}
    VersionList(const VersionList&) = delete;

    /* @brief Move constructs the version list
     * Note: use only at the beginning of the "other's" lifecycle since this
     * constructor doesn't move the RecordLock, but only the head pointer
     */
    VersionList(VersionList&& other) : id(other.id)
    {
        this->head = other.head.load();
        other.head = nullptr;
    }

    ~VersionList()
    {
        delete head.load();
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

    auto gc_lock_acquire()
    {
        return std::unique_lock<RecordLock>(lock);
    }

    void vacuum()
    {

    }

    T* find(const tx::Transaction& t) const
    {
        auto r = head.load(std::memory_order_seq_cst);

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
            r = r->next(std::memory_order_seq_cst);

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

        head.store(v1, std::memory_order_seq_cst);

        return v1;
    }

    T* update(tx::Transaction& t)
    {
        assert(head != nullptr);
        auto record = find(t);

        // check if we found any visible records
        if(!record)
            return nullptr;

        return update(record, t);
    }

    T* update(T* record, tx::Transaction& t)
    {
        assert(record != nullptr);
        lock_and_validate(record, t);

        auto updated = new T();
        updated->data = record->data;

        updated->mark_created(t);
        record->mark_deleted(t);

        updated->next(record, std::memory_order_seq_cst);
        head.store(updated, std::memory_order_seq_cst);

        return updated;
    }

    bool remove(tx::Transaction& t)
    {
        assert(head != nullptr);
        auto record = find(t);

        if(!record)
            return false;

        lock_and_validate(record, t);
        return remove(record, t), true;
    }

    bool remove(T* record, tx::Transaction& t)
    {
        assert(record != nullptr);
        lock_and_validate(record, t);
        record->mark_deleted(t);
        return true;
    }

    const Id id;

private:
    void lock_and_validate(T* record, tx::Transaction& t)
    {
        assert(record != nullptr);
        assert(record == find(t));

        // take a lock on this node
        t.take_lock(lock);

        // if the record hasn't been deleted yet or the deleting transaction
        // has aborted, it's ok to modify it
        if(!record->tx.exp() || !record->exp_committed(t))
            return;

        // if it committed, then we have a serialization conflict
        assert(record->hints.load().exp.is_committed());
        throw SerializationError();
    }

    std::atomic<T*> head {nullptr};
    RecordLock lock;
};

}

class Vertex;
class Edge;

using VertexRecord = mvcc::VersionList<Vertex>;
using EdgeRecord = mvcc::VersionList<Edge>;
