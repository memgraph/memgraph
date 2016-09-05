#pragma once

#include "utils/total_ordering.hpp"

class Void : public TotalOrdering<Void>
{
public:
    static Void _void;

    friend bool operator<(const Void &lhs, const Void &rhs) { return false; }

    friend bool operator==(const Void &lhs, const Void &rhs) { return true; }
};
