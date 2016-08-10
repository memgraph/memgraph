#pragma once

#include "mvcc/id.hpp"
#include "mvcc/version.hpp"
#include "threading/sync/lockable.hpp"
#include "transactions/transaction.hpp"

namespace mvcc
{

// TODO: this can be deleted !! DEPRICATED !!

template <class T>
class Atom : public Version<T>, public Lockable<SpinLock>
{
public:
    Atom(const Id &id, T *first) : Version<T>(first), id(id)
    {
        // it's illegal that the first version is nullptr. there should be at
        // least one version of a record
        assert(first != nullptr);
    }

    T *first() { return this->newer(); }

    // inspects the record change history and returns the record version visible
    // to the current transaction if it exists, otherwise it returns nullptr
    T *latest_visible(const tx::Transaction &t)
    {
        return first()->latest_visible(t);
    }

    // every record has a unique id. 2^64 = 1.8 x 10^19. that should be enough
    // for a looong time :) but keep in mind that some vacuuming would be nice
    // to reuse indices for deleted nodes.
    Id id;

    std::atomic<Atom<T> *> next;
};
}
