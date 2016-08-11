#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/common.hpp"
#include "storage/edge_accessor.hpp"

class Edges
{
public:
    Edge::Accessor find(tx::Transaction &t, const Id &id);
    Edge::Accessor insert(tx::Transaction &t);

private:
    ConcurrentMap<uint64_t, EdgeRecord> edges;
    AtomicCounter<uint64_t> counter;
};
