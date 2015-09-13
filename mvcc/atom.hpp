#ifndef MEMGRAPH_MVCC_ATOM_HPP
#define MEMGRAPH_MVCC_ATOM_HPP

#include "threading/sync/lockable.hpp"

#include "transaction.hpp"
#include "version.hpp"

namespace mvcc
{

template <class T>
class Atom : public Version<T>,
             public Lockable<>
{
public:
    Atom(uint64_t id, T* first)
        : Version<T>(first), id(id)
    {
        // it's illegal that the first version is nullptr. there should be at
        // least one version of a record
        assert(first != nullptr);
    }

    T* first()
    {
        return this->newer();
    }

    // inspects the record change history and returns the record version visible
    // to the current transaction if it exists, otherwise it returns nullptr
    T* latest_visible(const Transaction& t)
    {
        return first()->latest_visible(t);
    }

    // inspects the record change history and returns the newest available
    // version from all transactions
    T& newest_available()
    {
        T* record = this->newer(), newer = this->newer();

        while(newer != nullptr)
            record = newer, newer = record->newer();

        return record;
    }

    // every record has a unique id. 2^64 = 1.8 x 10^19. that should be enough
    // for a looong time :) but keep in mind that some vacuuming would be nice
    // to reuse indices for deleted nodes.
    uint64_t id;
};

}

#endif
