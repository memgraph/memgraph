#pragma once

#include "floating.hpp"

struct Double : public Floating<Double>
{
    static constexpr Flags type = Flags::Double;

    Double(double value) : Floating(Flags::Double), value(value) {}

    double value;
};

