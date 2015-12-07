#pragma once

#include "primitive.hpp"

namespace json {

class Integral final : public Primitive<int64_t>
{
public:
    Integral() {}

    Integral(int64_t value)
        : Primitive<int64_t>(value) {}

    virtual bool is_integral() const;

    virtual operator std::string() const;  
};

bool Integral::is_integral() const
{
    return true;
}

Integral::operator std::string() const
{
    return std::to_string(value);
}

}
