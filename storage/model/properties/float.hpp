#pragma once

#include "floating.hpp"

class Float : public Floating<Float>
{
    static constexpr Flags type = Flags::Float;

    Float(float value) : Floating(Flags::Float), value(value) {}

    float value;
};
