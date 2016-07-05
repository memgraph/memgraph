#pragma once

#include "data_structures/concurrent/skiplist.hpp"

template<class T>
class ConcurrentSet : public SkipList<T>
{
};
