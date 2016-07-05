#pragma once

#include <memory>
#include <string>
#include <cassert>
#include <ostream>
#include <vector>

#include "utils/underlying_cast.hpp"

class Null;

class Property
{
public:
    using sptr = std::shared_ptr<Property>;

    enum class Flags : unsigned
    {
        // Type       | Mask
        // -----------+----------------------------------------
        // Null       | 0000 0000 0000 0000 0000 0000 0000 0000
        // -----------+----------------------------------------
        // Bool       | 0000 0000 0000 0000 0000 0000 0000 0001
        // + True     | 0000 0000 0000 0000 0000 0000 0000 0011
        // + False    | 0000 0000 0000 0000 0000 0000 0000 0101
        // -----------+----------------------------------------
        // String     | 0000 0000 0000 0000 0000 0000 0000 1000
        // -----------+----------------------------------------
        // Number     | 0000 0000 0000 0000 0000 0000 0001 0000
        // + Integral | 0000 0000 0000 0000 0000 0000 0011 0000
        //  + Int32   | 0000 0000 0000 0000 0000 0000 0111 0000
        //  + Int64   | 0000 0000 0000 0000 0000 0000 1011 0000
        // + Floating | 0000 0000 0000 0000 0000 0001 0001 0000
        //  + Float   | 0000 0000 0000 0000 0000 0011 0001 0000
        //  + Double  | 0000 0000 0000 0000 0000 0101 0001 0000
        // -----------+----------------------------------------
        // Array      | 0000 0000 0000 0000 0001 0000 0000 0000
        // -----------+----------------------------------------

        Null     = 0x0,
        Bool     = 0x1,
        True     = 0x2 | Bool,
        False    = 0x4 | Bool,

        String   = 0x8,

        Number   = 0x10,
        Integral = 0x20 | Number,
        Int32    = 0x40 | Integral,
        Int64    = 0x80 | Integral,

        Floating = 0x100 | Number,
        Float    = 0x200 | Floating,
        Double   = 0x400 | Floating,

        Array    = 0x1000,

        type_mask = 0xFFF
    };

    static const Null Null;

    Property(Flags flags);

    virtual bool operator==(const Property& other) const = 0;

    bool operator!=(const Property& other) const;

    template <class T>
    bool is() const
    {
        return underlying_cast(flags) & underlying_cast(T::type);
    }

    template <class T>
    T& as()
    {
        assert(this->is<T>());
        return *static_cast<T*>(this);
    }

    template <class T>
    const T& as() const
    {
        assert(this->is<T>());
        return *static_cast<const T*>(this);
    }

    virtual std::ostream& print(std::ostream& stream) const = 0;

    friend std::ostream& operator<<(std::ostream& stream, const Property& prop);

//     template <class Handler>
//     void accept(Handler& h)
//     {
//         switch(flags)
//         {
//             case Flags::True:   return h.handle(static_cast<Bool&>(*this));
//             case Flags::False:  return h.handle(static_cast<Bool&>(*this));
//             case Flags::String: return h.handle(static_cast<String&>(*this));
//             case Flags::Int32:  return h.handle(static_cast<Int32&>(*this));
//             case Flags::Int64:  return h.handle(static_cast<Int64&>(*this));
//             case Flags::Float:  return h.handle(static_cast<Float&>(*this));
//             case Flags::Double: return h.handle(static_cast<Double&>(*this));
//             default: return;
//         }
//     }

    const Flags flags;
};

using properties_t = std::vector<Property::sptr>;
