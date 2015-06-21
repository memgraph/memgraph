#ifndef JSON_STRING_HPP
#define JSON_STRING_HPP

#include "primitive.hpp"

namespace json
{

class String final : public Primitive<std::string>
{
public:
    String() {}

    String(const std::string& value) 
        : Primitive<std::string>(value) {}

    virtual bool is_string() const;

    virtual operator std::string() const;
};

bool String::is_string() const
{
    return true;
}

String::operator std::string() const
{
    return "\"" + value + "\"";
}

}

#endif
