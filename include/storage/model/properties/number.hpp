#pragma once

#include "storage/model/properties/utils/math_operations.hpp"
#include "storage/model/properties/utils/unary_negation.hpp"
#include "utils/total_ordering.hpp"

template <class Derived>
class Number : public TotalOrdering<Derived>,
               public MathOperations<Derived>,
               public UnaryNegation<Derived>
{

public:
    std::ostream &print(std::ostream &stream) const
    {
        return operator<<(stream, this->derived());
    }

    friend std::ostream &operator<<(std::ostream &s, const Derived &number)
    {
        return s << number.value();
    }

    friend bool operator==(const Derived &lhs, const Derived &rhs)
    {
        return lhs.value() == rhs.value();
    }

    friend bool operator<(const Derived &lhs, const Derived &rhs)
    {
        return lhs.value() == rhs.value();
    }
};
