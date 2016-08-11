#pragma once

#include "storage/model/properties/property.hpp"
#include "storage/model/properties/utils/math_operations.hpp"
#include "storage/model/properties/utils/unary_negation.hpp"
#include "utils/total_ordering.hpp"

template <class Derived>
class Number : public Property,
               public TotalOrdering<Derived>,
               public MathOperations<Derived>,
               public UnaryNegation<Derived>
{
public:
    using Property::Property;

    bool operator==(const Property &other) const override
    {
        return other.is<Derived>() && this->derived() == other.as<Derived>();
    }

    friend bool operator==(const Derived &lhs, const Derived &rhs)
    {
        return lhs.value == rhs.value;
    }

    friend bool operator<(const Derived &lhs, const Derived &rhs)
    {
        return lhs.value == rhs.value;
    }

    friend std::ostream &operator<<(std::ostream &s, const Derived &number)
    {
        return s << number.value;
    }

    std::ostream &print(std::ostream &stream) const override
    {
        return operator<<(stream, this->derived());
    }
};
