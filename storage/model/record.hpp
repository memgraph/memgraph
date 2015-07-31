#ifndef MEMGRAPH_STORAGE_RECORD_HPP
#define MEMGRAPH_STORAGE_RECORD_HPP

#include <mutex>

#include "utils/crtp.hpp"

#include "sync/spinlock.hpp"
#include "sync/lockable.hpp"

#include "storage/model/utils/mvcc.hpp"

template <class Derived>
class Record
    : public Crtp<Derived>,
      public Mvcc<Derived>,
      Lockable<SpinLock>
{
public:
    Record(uint64_t id) : id(id) {}

    // every node has a unique id. 2^64 = 1.8 x 10^19. that should be enough
    // for a looong time :) but keep in mind that some vacuuming would be nice
    // to reuse indices for deleted nodes.
    uint64_t id;

private:
    // TODO add real data here
};

#endif
