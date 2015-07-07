#ifndef MEMGRAPH_DATA_MODEL_JSON_PRIMITIVE_HPP
#define MEMGRAPH_DATA_MODEL_JSON_PRIMITIVE_HPP

#include "json.hpp"

namespace json {

template <class T>
class Primitive : public Json
{
public:
    Primitive() {}

    Primitive(const T& value)
        : value(value) {}

    T get() const { return value; }
    void set(T value) { this->value = value; }

    operator T() const { return this->get(); }
    
protected:
    T value;
};


}

#endif
