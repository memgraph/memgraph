#ifndef MEMGRAPH_STORAGE_RECORD_HPP
#define MEMGRAPH_STORAGE_RECORD_HPP

#include <ostream>
#include <mutex>
#include <set>

#include "utils/crtp.hpp"

#include "threading/sync/spinlock.hpp"

#include "mvcc/mvcc.hpp"

#include "properties/properties.hpp"

template <class Derived>
class Record
    : public Crtp<Derived>,
      public mvcc::Mvcc<Derived>
{
public:
    // a record contains a key value map containing data
    model::Properties properties;
    
    // each record can have one or more distinct labels. 
    std::set<uint16_t> labels;
};

#endif
