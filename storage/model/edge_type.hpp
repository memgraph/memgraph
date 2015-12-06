#pragma once

#include <stdint.h>
#include <ostream>
#include "utils/total_ordering.hpp"

class EdgeType : public TotalOrdering<EdgeType>
{
public:
    EdgeType(const std::string& id) : id(id) {}
    EdgeType(std::string&& id) : id(std::move(id)) {}

    friend bool operator<(const EdgeType& lhs, const EdgeType& rhs)
    {
        return lhs.id < rhs.id;
    }

    friend bool operator==(const EdgeType& lhs, const EdgeType& rhs)
    {
        return lhs.id == rhs.id;
    }

    friend std::ostream& operator<<(std::ostream& stream, const EdgeType& type)
    {
       return stream << type.id;
    }

    operator const std::string&() const
    {
        return id;
    }

private:
    std::string id;
};
