#ifndef MEMGRAPH_DATA_MODEL_JSON_BOOL_HPP
#define MEMGRAPH_DATA_MODEL_JSON_BOOL_HPP

#include "primitive.hpp"

namespace json {

class Bool final : public Primitive<bool> 
{
public:
    Bool() {}

    Bool(bool value) 
        : Primitive<bool>(value) {}
    
    virtual bool is_boolean() const;

    virtual operator std::string() const;
};

bool Bool::is_boolean() const
{
    return true;
}

Bool::operator std::string() const
{
    return value == true ? "true" : "false";
}

}

#endif
