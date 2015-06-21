#ifndef JSON_NULL_HPP
#define JSON_NULL_HPP

#include "json.hpp"

namespace json {

class Null final : public Json 
{
public:
    Null() {}
    
    virtual bool is_null() const;

    virtual operator std::string() const;
};

bool Null::is_null() const
{
    return true;
}

Null::operator std::string() const
{
    return "null";
}
    
}

#endif
