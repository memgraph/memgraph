#include "storage/model/properties/bool.hpp"

Bool::Bool(bool value) : Property(value ? Flags::True : Flags::False) {}

bool Bool::value() const
{
    // true when the subtraction of True from flags is equal to zero

    // True  0000 0000  0000 0000  0000 0000  0000 0011
    // False 0000 0000  0000 0000  0000 0000  0000 0101
    //
    // True - True = 0
    // False - True != 0

    return (underlying_cast(flags) - underlying_cast(Flags::True)) == 0;
}

Bool::operator bool() const
{
    return value();
}

bool Bool::operator==(const Property& other) const
{
    return other.is<Bool>() && operator==(other.as<Bool>());
}

bool Bool::operator==(const Bool& other) const
{
    return other.flags == flags;
}

bool Bool::operator==(bool v) const
{
    return value() == v;
}

std::ostream& Bool::print(std::ostream& stream) const
{
    return operator<<(stream, *this);
}

std::ostream& operator<<(std::ostream& stream, const Bool& prop)
{
    return stream << prop.value();
}
