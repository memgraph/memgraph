#include "storage/label/label.hpp"

Label::Label(const std::string& name) : name(name) {}
Label::Label(std::string&& name) : name(std::move(name)) {}

bool operator<(const Label& lhs, const Label& rhs)
{
    return lhs.name < rhs.name;
}

bool operator==(const Label& lhs, const Label& rhs)
{
    return lhs.name == rhs.name;
}

std::ostream& operator<<(std::ostream& stream, const Label& label)
{
    return stream << label.name;
}

Label::operator const std::string&() const
{
    return name;
}
