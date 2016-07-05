#include "storage/model/properties/string.hpp"

String::String(const std::string& value) : Property(Flags::String), value(value) {}
String::String(std::string&& value) : Property(Flags::String), value(value) {}


String::operator const std::string&() const
{
    return value;
}

bool String::operator==(const Property& other) const
{
    return other.is<String>() && operator==(other.as<String>());
}

bool String::operator==(const String& other) const
{
    return value == other.value;
}

bool String::operator==(const std::string& other) const
{
    return value == other;
}

std::ostream& operator<<(std::ostream& stream, const String& prop)
{
    return stream << prop.value;
}

std::ostream& String::print(std::ostream& stream) const
{
    return operator<<(stream, *this);
}

