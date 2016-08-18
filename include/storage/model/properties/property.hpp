#pragma once

#include <cassert>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "storage/model/properties/flags.hpp"
#include "utils/underlying_cast.hpp"

class Null;

class Property
{
public:
    using sptr = std::shared_ptr<Property>;

    static const Null Null;

    Property(Flags flags);

    virtual bool operator==(const Property &other) const = 0;

    bool operator!=(const Property &other) const;

    template <class T>
    bool is() const
    {
        return underlying_cast(flags) & underlying_cast(T::type);
    }

    template <class T>
    T &as()
    {
        assert(this->is<T>());
        return *static_cast<T *>(this);
    }

    template <class T>
    const T &as() const
    {
        assert(this->is<T>());
        return *static_cast<const T *>(this);
    }

    virtual std::ostream &print(std::ostream &stream) const = 0;

    friend std::ostream &operator<<(std::ostream &stream, const Property &prop);

    const Flags flags;
};

using properties_t = std::vector<Property::sptr>;
