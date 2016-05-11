#pragma once

#include <stdint.h>
#include <ostream>
#include "utils/total_ordering.hpp"

class Label : public TotalOrdering<Label>
{
public:
    Label(const std::string& name) : name(name) {}
    Label(std::string&& name) : name(std::move(name)) {}

    Label(const Label&) = default;
    Label(Label&&) = default;

    friend bool operator<(const Label& lhs, const Label& rhs)
    {
        return lhs.name < rhs.name;
    }

    friend bool operator==(const Label& lhs, const Label& rhs)
    {
        return lhs.name == rhs.name;
    }

    friend std::ostream& operator<<(std::ostream& stream, const Label& label)
    {
        return stream << label.name;
    }

    operator const std::string&() const
    {
        return name;
    }

private:
    std::string name;
};
