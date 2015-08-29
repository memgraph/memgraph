#ifndef MEMGRAPH_STORAGE_MODEL_PROPERTIES_PROPERTY_HPP
#define MEMGRAPH_STORAGE_MODEL_PROPERTIES_PROPERTY_HPP

#include <memory>
#include <string>

class Property
{
public:
    // shared_ptr is being used because of MVCC - when you clone a record, you
    // clone it's properties. when a single property is updated, a lot of
    // memory is being wasted. this way it is shared until you need to change
    // something and shared ptr ensures it's being properly tracked and
    // cleaned up after no one is using it

    using sptr = std::shared_ptr<Property>;

    virtual ~Property() {}
    virtual void dump(std::string& buffer) = 0;
};

template <class T>
class Value : public Property
{
public:
    Value(T value) : value(value) {}
    T value;
};

#endif
