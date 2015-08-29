#ifndef MEMGRAPH_STORAGE_MODEL_ROOT_HPP
#define MEMGRAPH_STORAGE_MODEL_ROOT_HPP

#include "utils/version.hpp"

template <class T>
class Root : public Version<T>
{
public:
    Root(uint64_t id, T* first)
        : Version<T>(first), id(id) {}

    // every record has a unique id. 2^64 = 1.8 x 10^19. that should be enough
    // for a looong time :) but keep in mind that some vacuuming would be nice
    // to reuse indices for deleted nodes.
    uint64_t id;
};

#endif
