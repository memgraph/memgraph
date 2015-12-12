#pragma once

#include "edge.hpp"
#include "common.hpp"

class Edges
{
public:
    // TODO: implementation

private:
    SkipList<uint64_t, EdgeRecord> edges;
    AtomicCounter<uint64_t> counter;
};
