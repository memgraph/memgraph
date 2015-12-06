#pragma once

#include <stdint.h>
#include <ostream>
#include "utils/total_ordering.hpp"

class Label : public TotalOrdering<Label>
{
public:
    Label(const std::string& id) : id(id) {}
    Label(std::string&& id) : id(std::move(id)) {}

    friend bool operator<(const Label& lhs, const Label& rhs)
    {
        return lhs.id < rhs.id;
    }

    friend bool operator==(const Label& lhs, const Label& rhs)
    {
        return lhs.id == rhs.id;
    }

    friend std::ostream& operator<<(std::ostream& stream, const Label& label)
    {
        return stream << label.id;
    }

    operator const std::string&() const
    {
        return id;
    }

private:
    std::string id;
};
