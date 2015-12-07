#pragma once

#include <memory>
#include <string>

#include "utils/underlying_cast.hpp"

struct Property
{
    using sptr = std::shared_ptr<Property>;

    enum class Flags : unsigned
    {
        Null     = 0x1,

        Bool     = 0x2,
        True     = 0x4 | Bool,
        False    = 0x8 | Bool,

        String   = 0x10,

        Number   = 0x20,
        Integral = 0x40  | Number,
        Int32    = 0x80  | Integral,
        Int64    = 0x100 | Integral,

        Floating = 0x200 | Number,
        Float    = 0x400 | Floating,
        Double   = 0x800 | Floating,

        Array    = 0x1000,

        type_mask = 0xFFF
    };

    Property(Flags flags) : flags(flags) {}

    Property(const Property&) = default;

    template <class T>
    T* is()
    {
        return underlying_cast(flags) & T::type;
    }

    template <class T>
    T* as()
    {
        if(this->is<T>())
            return static_cast<T*>(this);

        return nullptr;
    }

    template <class Handler>
    void accept(Handler& handler);

    Flags flags;
};

struct Null : public Property
{
    static constexpr Flags type = Flags::Null;

    Null() : Property(Flags::Null) {}

    bool is_null()
    {
        return true;
    }
};

struct Bool : public Property
{
    static constexpr Flags type = Flags::Bool;

    Bool(bool value) : Property(value ? Flags::True : Flags::False) {}

    bool value()
    {
        unsigned flags = underlying_cast(this->flags);
        unsigned true_t = underlying_cast(Flags::True);

        return (flags - true_t) == 0;
    }
};

struct String : public Property
{
    static constexpr Flags type = Flags::String;

    String(const std::string& value) 
        : Property(Flags::String), value(value) {}

    String(std::string&& value)
        : Property(Flags::String), value(value) {}

    std::string value;
};

struct Int32 : public Property
{
    static constexpr Flags type = Flags::Int32;

    Int32(int32_t value) 
        : Property(Flags::Int32), value(value) {}

    int32_t value;
};

struct Int64 : public Property
{
    static constexpr Flags type = Flags::Int64;

    Int64(int64_t value) 
        : Property(Flags::Int64), value(value) {}

    int64_t value;
};

struct Float : public Property
{
    static constexpr Flags type = Flags::Float;

    Float(float value) 
        : Property(Flags::Float), value(value) {}

    float value;
};

struct Double : public Property
{
    static constexpr Flags type = Flags::Double;

    Double(double value) 
        : Property(Flags::Double), value(value) {}

    double value;
};

template <class Handler>
void Property::accept(Handler& h)
{
    switch(flags)
    {
        case Flags::Null:   return h.handle(static_cast<Null&>(*this));
        case Flags::True:   return h.handle(static_cast<Bool&>(*this));
        case Flags::False:  return h.handle(static_cast<Bool&>(*this));
        case Flags::String: return h.handle(static_cast<String&>(*this));
        case Flags::Int32:  return h.handle(static_cast<Int32&>(*this));
        case Flags::Int64:  return h.handle(static_cast<Int64&>(*this));
        case Flags::Float:  return h.handle(static_cast<Float&>(*this));
        case Flags::Double: return h.handle(static_cast<Double&>(*this));
        default: return;
    }
}
