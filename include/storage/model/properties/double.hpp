#pragma once

#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/floating.hpp"

struct Double : public Floating<Double>
{
public:
    const static Type type;

    Double(double d) : data(d) {}

    double &value() { return data; }

    double const &value() const { return data; }

private:
    double data;
};
