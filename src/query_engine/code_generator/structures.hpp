#pragma once

#include <vector>

#include "utils/assert.hpp"

template <class T>
class Vector : public std::vector<T>
{
public:
    using pair = std::pair<T, T>;

    pair last_two()
    {
        runtime_assert(this->size() > 1, "Array size shoud be bigger than 1");

        return std::make_pair(*(this->end() - 1), *(this->end() - 2));
    }
};
