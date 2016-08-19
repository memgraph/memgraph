#pragma once

#include "storage/model/properties/floating.hpp"

struct Double : public Floating<Double>
{
    static constexpr Flags type = Flags::Double;

    Double(double value) : Floating(Flags::Double), value(value) {}

    double const &value_ref() const { return value; }

    double value;
};
