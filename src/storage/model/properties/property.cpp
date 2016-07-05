#include "storage/model/properties/property.hpp"

Property::Property(Flags flags) : flags(flags) {}

bool Property::operator!=(const Property& other) const
{
    return !operator==(other);
}

std::ostream& operator<<(std::ostream& stream, const Property& prop)
{
    return prop.print(stream);
}
