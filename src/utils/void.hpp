#pragma once

#include "utils/total_ordering.hpp"

// Type which represents nothing.
class Void : public TotalOrdering<Void>
{
public:
    friend bool operator<(const Void &lhs, const Void &rhs) { return false; }

    friend bool operator==(const Void &lhs, const Void &rhs) { return true; }
};

static Void _void = {};
