#include "storage/model/properties/string.hpp"

const Type String::type = Type(Flags::String);

bool String::operator==(const String &other) const
{
    return value() == other.value();
}

bool String::operator==(const std::string &other) const
{
    return value() == other;
}

std::ostream &operator<<(std::ostream &stream, const String &prop)
{
    return stream << prop.value();
}

std::ostream &String::print(std::ostream &stream) const
{
    return operator<<(stream, *this);
}
