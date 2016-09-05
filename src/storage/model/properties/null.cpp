#include "storage/model/properties/null.hpp"

const Type Null::type = Type(Flags::Null);

bool Null::operator==(const Null &) const { return true; }

Null::operator bool() { return false; }

std::ostream &operator<<(std::ostream &stream, const Null &)
{
    return stream << "NULL";
}

std::ostream &Null::print(std::ostream &stream) const
{
    return operator<<(stream, *this);
}
