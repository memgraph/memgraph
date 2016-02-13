#pragma once

#include "property.hpp"

class Bool : public Property
{
public:
    static constexpr Flags type = Flags::Bool;

    Bool(bool value) : Property(value ? Flags::True : Flags::False) {}

    Bool(const Bool& other) = default;

    bool value() const
    {
        // true when the subtraction of True from flags is equal to zero

        // True  0000 0000  0000 0000  0000 0000  0000 0011
        // False 0000 0000  0000 0000  0000 0000  0000 0101
        //
        // True - True = 0
        // False - True != 0

        return (underlying_cast(flags) - underlying_cast(Flags::True)) == 0;
    }

    explicit operator bool() const
    {
        return value();
    }

    bool operator==(const Property& other) const override
    {
        return other.is<Bool>() && operator==(other.as<Bool>());
    }

    bool operator==(const Bool& other) const
    {
        return other.flags == flags;
    }

    bool operator==(bool v) const
    {
        return value() == v;
    }
};

