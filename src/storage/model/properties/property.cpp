#include "storage/model/properties/property.hpp"

#include "storage/model/properties/all.hpp"

Property Property::handle(Void &&v)
{
    return Property(Null(), Type(Flags::Null));
}

Property Property::handle(bool &&prop)
{
    return Property(Bool(prop), Type(Flags::Bool));
}

Property Property::handle(float &&prop)
{
    return Property(Float(prop), Type(Flags::Float));
}

Property Property::handle(double &&prop)
{
    return Property(Double(prop), Type(Flags::Double));
}

Property Property::handle(int32_t &&prop)
{
    return Property(Int32(prop), Type(Flags::Int32));
}

Property Property::handle(int64_t &&prop)
{
    return Property(Int64(prop), Type(Flags::Int64));
}

Property Property::handle(std::string &&value)
{
    return Property(String(std::move(value)), Type(Flags::String));
}

Property Property::handle(ArrayStore<bool> &&a)
{
    return Property(ArrayBool(std::move(a)), Type(Flags::ArrayBool));
}

Property Property::handle(ArrayStore<int32_t> &&a)
{
    return Property(ArrayInt32(std::move(a)), Type(Flags::ArrayInt32));
}

Property Property::handle(ArrayStore<int64_t> &&a)
{
    return Property(ArrayInt64(std::move(a)), Type(Flags::ArrayInt64));
}

Property Property::handle(ArrayStore<float> &&a)
{
    return Property(ArrayFloat(std::move(a)), Type(Flags::ArrayFloat));
}

Property Property::handle(ArrayStore<double> &&a)
{
    return Property(ArrayDouble(std::move(a)), Type(Flags::ArrayDouble));
}

Property Property::handle(ArrayStore<std::string> &&a)
{
    return Property(ArrayString(std::move(a)), Type(Flags::ArrayString));
}
