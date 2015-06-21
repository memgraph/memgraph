#ifndef JSON_REAL_HPP
#define JSON_REAL_HPP

#include "primitive.hpp"

namespace json {

class Real final : public Primitive<float> 
{
public:
    Real() {}

    Real(float value)
        : Primitive<float>(value) {}
    
    virtual bool is_real() const;

    virtual operator std::string() const;
};

bool Real::is_real() const
{
    return true;
}

Real::operator std::string() const
{
    return std::to_string(value);
}

}

#endif
