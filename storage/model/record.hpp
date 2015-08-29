#ifndef MEMGRAPH_STORAGE_RECORD_HPP
#define MEMGRAPH_STORAGE_RECORD_HPP

#include <mutex>

#include "utils/crtp.hpp"

#include "sync/spinlock.hpp"
#include "sync/lockable.hpp"

#include "storage/model/utils/mvcc.hpp"

#include "properties/properties.hpp"

template <class Derived>
class Record
    : public Crtp<Derived>,
      public Mvcc<Derived>,
      Lockable<SpinLock>
{
public:
    Properties props;
};

#endif
