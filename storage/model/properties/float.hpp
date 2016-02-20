#pragma once

#include "floating.hpp"
#include "double.hpp"

class Float : public Floating<Float>
{
public:
    static constexpr Flags type = Flags::Float;

    Float(float value) : Floating(Flags::Float), value(value) {}

    operator Double() const
    {
        return Double(value);
    }

    float value;
};
