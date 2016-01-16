#pragma once

#include "common.hpp"
#include "edge_accessor.hpp"

class Edges
{
public:
    Edge::Accessor find(tx::Transaction& t, const Id& id)
    {
        throw std::runtime_error("not implemented");
    }

    Edge::Accessor insert(tx::Transaction& t)
    {
        throw std::runtime_error("not implemented");
    }

private:
    SkipList<uint64_t, EdgeRecord> edges;
    AtomicCounter<uint64_t> counter;
};
