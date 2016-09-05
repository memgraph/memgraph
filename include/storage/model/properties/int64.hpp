#pragma once

#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/integral.hpp"

class Int64 : public Integral<Int64>
{
public:
    const static Type type;

    Int64(int64_t d) : data(d) {}

    int64_t &value() { return data; }

    int64_t const &value() const { return data; }
private:
    int64_t data;
};
