#pragma once

#include <ostream>
#include <stdint.h>

#include "utils/total_ordering.hpp"

class Id : public TotalOrdering<Id>
{
public:
    Id() = default;

    Id(uint64_t id);

    friend bool operator<(const Id& a, const Id& b);

    friend bool operator==(const Id& a, const Id& b);

    friend std::ostream& operator<<(std::ostream& stream, const Id& id);

    operator uint64_t() const;

private:
    uint64_t id {0};
};
