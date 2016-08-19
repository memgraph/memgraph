#pragma once

#include "storage/model/properties/int64.hpp"
#include "storage/model/properties/integral.hpp"

class Int32 : public Integral<Int32>
{
public:
    static constexpr Flags type = Flags::Int32;

    Int32(int32_t value) : Integral(Flags::Int32), value(value) {}

    operator Int64() const { return Int64(value); }

    int32_t const &value_ref() const { return value; }

    int32_t value;
};
