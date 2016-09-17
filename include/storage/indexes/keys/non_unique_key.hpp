#pragma once

// TODO: DEPRICATED

#include "unique_key.hpp"

template <class K, class T, class SortOrder>
class NonUniqueKey
{
public:
    NonUniqueKey(const K& key, const T&)
    {

    }

private:
    intptr_t x;
};
