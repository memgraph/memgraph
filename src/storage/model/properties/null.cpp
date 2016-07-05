#include "storage/model/properties/null.hpp"

bool Null::operator==(const Property& other) const
{
    return other.is<Null>();
}

bool Null::operator==(const Null&) const
{
    return true;
}

Null::operator bool()
{
    return false;
}

std::ostream& operator<<(std::ostream& stream, const Null&)
{
    return stream << "NULL";
}

std::ostream& Null::print(std::ostream& stream) const
{
    return operator<<(stream, *this);
}

Null::Null() : Property(Flags::Null) {}

const Null Property::Null;
