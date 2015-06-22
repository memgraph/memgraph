#ifndef MEMGRAPH_DATA_MODEL_RECORD_HPP
#define MEMGRAPH_DATA_MODEL_RECORD_HPP

#include <cstdlib>

class Record
{
    uint64_t id;

    // used by MVCC to keep track of what's visible to transactions
    uint64_t xmin, xmax;
};

#endif
