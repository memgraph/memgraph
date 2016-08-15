#pragma once

#include "storage/model/properties/double.hpp"
#include "storage/model/properties/floating.hpp"

class Float : public Floating<Float>
{
public:
    static constexpr Flags type = Flags::Float;

    Float(float value) : Floating(Flags::Float), value(value) {}

    operator Double() const { return Double(value); }

    float const &value_ref() const { return value; }

    float value;
};
