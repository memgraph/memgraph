#pragma once

#include "storage/model/properties/double.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/floating.hpp"

class Float : public Floating<Float>
{

public:
    const static Type type;

    Float(float d) : data(d) {}

    operator Double() const { return Double(value()); }

    float &value() { return data; }

    float const &value() const { return data; }

private:
    float data;
};
