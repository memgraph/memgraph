#ifndef MEMGRAPH_STORAGE_MVCC_STORE_HPP
#define MEMGRAPH_STORAGE_MVCC_STORE_HPP

#include <stdexcept>

#include "mvcc/transaction.hpp"
#include "mvcc/atom.hpp"

#include "data_structures/list/lockfree_list.hpp"
#include "utils/counters/atomic_counter.hpp"

// some interesting concepts described there, keep in mind for the future
// Serializable Isolation for Snapshot Databases, J. Cahill, et al.

namespace mvcc
{

class MvccError : public std::runtime_error
{
public:
    using runtime_error::runtime_error;
};

template <class T>
class MvccStore
{
    using list_t = lockfree::List<Atom<T>>;

public:
    using iterator = typename list_t::iterator;

    MvccStore() : counter(0) {}

    iterator insert(const Transaction& t)
    {
        auto record = new T();

        record->mark_created(t);

        return data.push_front(Atom<T>(counter.next(), record));
    }

    T* update(Atom<T>& atom, T& record, const Transaction& t)
    {
        auto guard = atom.acquire();

        // if xmax is not zero, that means there is a newer version of this
        // record or it has been deleted. we cannot do anything here
        if(record.tx.max())
            throw MvccError("can't serialize due to concurrent operation(s)");

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

    void remove(Atom<T>& atom, T& record, const Transaction& t)
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

#endif
