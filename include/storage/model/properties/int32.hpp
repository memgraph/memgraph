#pragma once

#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/int64.hpp"
#include "storage/model/properties/integral.hpp"

class Int32 : public Integral<Int32>
{
public:
    const static Type type;

    Int32(int32_t d) : data(d) {}

    operator Int64() const { return Int64(value()); }

    int32_t &value() { return data; }

    int32_t const &value() const { return data; }

private:
    int32_t data;
};
