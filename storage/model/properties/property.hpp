#ifndef MEMGRAPH_STORAGE_MODEL_PROPERTIES_PROPERTY_HPP
#define MEMGRAPH_STORAGE_MODEL_PROPERTIES_PROPERTY_HPP

#include <memory>
#include <string>

namespace props
{

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

    template <class T>
    T* as()
    {
        // return dynamic_cast<T*>(this);

        // http://stackoverflow.com/questions/579887/how-expensive-is-rtti
        // so... typeid is 20x faster! but there are some caveats, use with
        // caution. read CAREFULLY what those people are saying.
        // should be ok to use in this situation because all types used by
        // this comparison are local and compile together with this code
        // and we're compiling it only for linux with gcc/clang and we will
        // not use any classes from third party libraries in this function.
        if(typeid(T*) == typeid(this))
            return static_cast<T*>(this);

        return nullptr;
    }
};

template <class T>
class Value : public Property
{
public:
    Value(T value) : value(value) {}
    T value;
};

}

#endif
