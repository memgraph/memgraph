#pragma once

#include "transactions/transaction.hpp"
#include "mvcc/atom.hpp"
#include "mvcc/mvcc_error.hpp"

#include "data_structures/list/lockfree_list.hpp"
#include "utils/counters/atomic_counter.hpp"

// some interesting concepts described there, keep in mind for the future
// Serializable Isolation for Snapshot Databases, J. Cahill, et al.

namespace mvcc
{

template <class T>
class MvccStore
{
    using list_t = lockfree::List<Atom<T>>;

public:
    using read_iterator = typename list_t::read_iterator;
    using read_write_iterator = typename list_t::read_write_iterator;

    MvccStore() : counter(0) {}

    read_iterator begin()
    {
        return data.begin();
    }

    read_write_iterator rw_begin()
    {
        return data.rw_begin();
    }

    Atom<T>* insert(const tx::Transaction& t)
    {
        // create a first version of the record
        auto record = new T();

        // mark the record as created by the transaction t
        record->mark_created(t);

        // create an Atom to put in the list
        auto atom = new Atom<T>(counter.next(), record);

        // put the atom with the record to the linked list
        data.push_front(atom);

        return atom;
    }

    T* update(Atom<T>& atom, T& record, const tx::Transaction& t)
    {
        auto guard = atom.acquire();

        // if xmax is not zero, that means there is a newer version of this
        // record or it has been deleted. we cannot do anything here until
        // we implement some more intelligent locking mechanisms
        if(record.tx.max())
            throw MvccError("can't serialize due to concurrent operation(s)");

        assert(atom.latest_visible(t) == &record);
        assert(atom.latest_visible(t) == record.latest_visible(t));

        // make a new version
        auto updated = new T();
        *updated = *record;

        // mark the new version as created
        updated->mark_created(t);

        // mark the current version as deleted
        record.mark_deleted(t);
        record.newer(updated);

        return updated;
    }

    void remove(Atom<T>& atom, T& record, const tx::Transaction& t)
    {
        auto guard = atom.acquire();

        // if xmax is not zero, that means there is a newer version of this
        // record or it has been deleted. we cannot do anything here
        if(record.tx.max())
            throw MvccError("can't serialize due to concurrent operation(s)");

        record.mark_deleted(t);
    }

private:
    AtomicCounter<uint64_t> counter;
    lockfree::List<Atom<T>> data;
};

}
